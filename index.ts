import 'dotenv/config';
import { readdirSync, writeFileSync, appendFileSync, existsSync, readFileSync, mkdirSync } from 'fs';
import { join } from 'path';
import crypto from 'crypto';
import { config } from './config.js';
import { sqlQuery } from './utils/database.js';
import { listAllFolders, downloadFolder, rcloneCopy, doesS3Exist } from './utils/s3.js';
import { 
  bashCommand, 
  shellQuote, 
  removeDuplicatesFile, 
  extractDateFromFilename,
  readGzippedFile,
  readTextFile,
  ensureDirectoryExists,
  removeDirectory,
  removeFile
} from './utils/fileUtils.js';
import { whatIsGender, loadHtmlMap, identifyItem } from './utils/urlIdentifier.js';

interface LogEntry {
  db?: string;
  id?: string;
  ip: string;
  date: string;
}

// Parse command line arguments
const genderGet = process.argv[2];
if (!genderGet) {
  console.error('Usage: node index.js <gender>');
  console.error('Gender must be: f, m, or fans');
  process.exit(1);
}

const genderType = whatIsGender(genderGet);
if (!genderType) {
  console.error('Bad gender here! Must be: f, m, or fans');
  process.exit(1);
}

let bucketName: string;
let bucketNameClean: string;
let databaseGlobal: string;
let genderGlobalLog: string;

if (genderType === 'f') {
  genderGlobalLog = genderType;
  bucketName = 'aznude-logs';
  bucketNameClean = 'aznude-clean-logs';
  databaseGlobal = config.databases.AZNUDE;
} else if (genderType === 'm') {
  genderGlobalLog = genderType;
  bucketName = 'azmen-logs';
  bucketNameClean = 'azmen-clean-logs';
  databaseGlobal = config.databases.AZNUDEMEN;
} else if (genderType === 'fans') {
  genderGlobalLog = genderType;
  bucketName = 'azfans-logs';
  bucketNameClean = 'azfans-clean-logs';
  databaseGlobal = config.databases.AZFANS;
} else {
  console.error('Bad gender here!');
  process.exit(1);
}

// Load HTML map associations
console.log('Loading HTML map associations...');
await loadHtmlMap();
console.log('HTML map loaded');

// Use local logs folder for notfound file
const notFoundFile = `./logs/notfound-${genderGlobalLog}.txt`;

async function parseLargeJsonFile(filePath: string): Promise<LogEntry[]> {
  const dateHi = filePath.split('/').pop() || '';
  const dateGet = extractDateFromFilename(dateHi);
  const entries: LogEntry[] = [];

  if (!dateGet) {
    console.error('Could not extract date from filename:', dateHi);
    return entries;
  }

  // Batch SSH file checks for performance
  interface UnidentifiedFile {
    host: string;
    path: string;
    serverPath: string;
  }
  const unidentifiedFiles: UnidentifiedFile[] = [];
  const BATCH_SIZE = 50; // Check 50 files per SSH command

  // Determine if file is gzipped
  const isGzipped = filePath.endsWith('.gz');
  const fileIterator = isGzipped ? readGzippedFile(filePath) : readTextFile(filePath);

  let lineCount = 0;
  for await (const line of fileIterator) {
    lineCount++;
    if (lineCount % 10000 === 0) {
      console.log(`Processed ${lineCount} lines from ${filePath}`);
    }

    try {
      const data = JSON.parse(line);
      let clientRequestHost = data.ClientRequestHost;
      const clientRequestPath = data.ClientRequestPath;
      let clientIP = data.ClientIP;

      if (!clientIP) {
        continue;
      }

      // Hash IP address
      clientIP = crypto.createHash('md5').update(String(clientIP)).digest('hex');

      // Normalize host
      if (clientRequestHost === 'aznude.com' || clientRequestHost === 'azmen.com' || clientRequestHost === 'azfans.com') {
        clientRequestHost = 'www.' + clientRequestHost;
      }

      // console.log(clientRequestHost);

      // Determine directory and gsutil based on host
      let dir: string, gsutil: string, gsutilDomain: string;
      
      if (['cdn2.aznude.com', 'cdn1.aznude.com', 'www.aznude.com', 'user-uploads.aznude.com', 'aznude.com'].includes(clientRequestHost) || 
          clientRequestHost.includes('aznude.com')) {
        dir = config.directories.MAIN_DIR_AZNUDE;
        gsutil = config.gsutil.GC_WOMEN_HTML;
        gsutilDomain = gsutil.replace('gs://', '');
      } else if (['men.aznude.com', 'cdn-men.aznude.com', 'azmen.com', 'www.azmen.com'].includes(clientRequestHost) || 
                 clientRequestHost.includes('azmen.com')) {
        dir = config.directories.MAIN_DIR_AZNUDEMEN;
        gsutil = config.gsutil.GC_MEN_HTML;
        gsutilDomain = gsutil.replace('gs://', '');
      } else if (['cdn2.azfans.com', 'azfans.com', 'www.azfans.com'].includes(clientRequestHost) || 
                 clientRequestHost.includes('azfans.com')) {
        dir = config.directories.MAIN_DIR_AZFANS;
        gsutil = config.gsutil.GC_FANS_HTML;
        gsutilDomain = gsutil.replace('gs://', '');
      } else {
        // Default fallback
        dir = config.directories.MAIN_DIR_AZNUDE;
        gsutil = config.gsutil.GC_WOMEN_HTML;
        gsutilDomain = gsutil.replace('gs://', '');
      }

      if (clientRequestHost && clientRequestPath) {
        const resultIdentify = await identifyItem(clientRequestHost, clientRequestPath);

        if (resultIdentify === 'unidentified') {
          // Match Python logic: full_thing.replace(gsutil_domain, dir)
          // However, this replace won't work if gsutil_domain isn't in full_thing
          // The Python code has a bug - gsutil_domain (e.g., "aznude-html") is not in full_thing (e.g., "www.aznude.com/view/...")
          // So we fix it: convert URL to server path by removing host and prepending directory
          const fullThing = clientRequestHost + clientRequestPath;
          let fullThingServer: string;
          
          // Try Python's replace first (in case it works in some edge cases)
          fullThingServer = fullThing.replace(gsutilDomain, dir);
          
          // If replace didn't change anything (gsutil_domain not found), manually convert
          // This fixes the Python bug where replace doesn't work
          if (fullThingServer === fullThing) {
            // Convert URL to server path: remove host, prepend directory
            // Example: "www.aznude.com/view/celeb/e/elisabrandani.html" -> "/var/www/html/aznbaby/view/celeb/e/elisabrandani.html"
            if (clientRequestPath.startsWith('/')) {
              fullThingServer = dir + clientRequestPath;
            } else {
              fullThingServer = dir + '/' + clientRequestPath;
            }
          }
          // Add to batch instead of checking immediately
          unidentifiedFiles.push({
            host: clientRequestHost,
            path: clientRequestPath,
            serverPath: fullThingServer
          });
          
          // Process batch when it reaches BATCH_SIZE
          if (unidentifiedFiles.length >= BATCH_SIZE) {
            await processBatchSSHChecks(unidentifiedFiles, notFoundFile);
            unidentifiedFiles.length = 0; // Clear the batch
          }
        } else if (resultIdentify !== 'invalid') {
          // Create ordered result with IP and date at the end
          const orderedResult: LogEntry = {
            ...resultIdentify,
            ip: String(clientIP),
            date: String(dateGet)
          };
          entries.push(orderedResult);
        }
      }
    } catch (error) {
      console.error('Error parsing line:', error);
      continue;
    }
  }

  // Process remaining batch items
  if (unidentifiedFiles.length > 0) {
    await processBatchSSHChecks(unidentifiedFiles, notFoundFile);
  }

  return entries;
}

// Batch process SSH file existence checks
async function processBatchSSHChecks(
  files: Array<{ host: string; path: string; serverPath: string }>,
  notFoundFile: string
): Promise<void> {
  if (files.length === 0) return;
  
  // Build a single SSH command that checks all files
  // Format: ssh user@host "for file in 'path1' 'path2' ...; do test -f \"$file\" && echo \"EXISTS:$file\" || echo \"NOTEXISTS:$file\"; done"
  const filePaths = files.map(f => shellQuote(f.serverPath)).join(' ');
  const command = `${config.LEASEWEB_SERVER_SSH} "for file in ${filePaths}; do test -f \"\\$file\" && echo \"EXISTS:\\$file\" || echo \"NOTEXISTS:\\$file\"; done"`;
  
  let attemptsFileCheck = 0;
  let unexpectedError = 0;

  while (true) {
    try {
      const normalizedCommand = Buffer.from(command, 'utf-8').toString('utf-8');
      console.log(`Batch checking ${files.length} files via SSH...`);
      
      const outputCommand = bashCommand(normalizedCommand);
      
      // Parse results - each line is either "EXISTS:path" or "NOTEXISTS:path"
      const results = new Map<string, boolean>();
      const lines = outputCommand.split('\n');
      
      for (const line of lines) {
        if (line.startsWith('EXISTS:')) {
          const path = line.substring(7).trim();
          results.set(path, true);
        } else if (line.startsWith('NOTEXISTS:')) {
          const path = line.substring(10).trim();
          results.set(path, false);
        }
      }
      
      // Process results and write to notfound.txt
      const notFoundDir = notFoundFile.substring(0, notFoundFile.lastIndexOf('/'));
      if (!existsSync(notFoundDir)) {
        mkdirSync(notFoundDir, { recursive: true });
      }
      
      let foundCount = 0;
      for (const file of files) {
        const exists = results.get(file.serverPath);
        if (exists === true) {
          appendFileSync(notFoundFile, file.host + file.path + '\n');
          foundCount++;
        }
      }
      
      if (foundCount > 0) {
        console.log(`Batch result: ${foundCount}/${files.length} files exist on server`);
      }
      
      break; // Success
      
    } catch (error: any) {
      const errorMessage = error.message || String(error);
      const errorOutput = error.stderr || error.output?.[2] || '';
      
      // Check if it's a persistent SSH authentication error
      if (errorMessage.includes('Permission denied (publickey)') || 
          errorOutput.includes('Permission denied (publickey)')) {
        console.warn(`SSH Authentication Failed for batch - skipping ${files.length} file checks`);
        break; // Skip batch, continue processing
      }
      
      console.error(`Batch SSH check failed (attempt ${attemptsFileCheck + 1}):`, errorMessage);
      unexpectedError++;
      await new Promise(resolve => setTimeout(resolve, 10000));
      
      if (unexpectedError >= 10) {
        console.error('Batch SSH check failed 10 times - skipping batch');
        break; // Skip batch after 10 failures
      }
      
      attemptsFileCheck++;
      if (attemptsFileCheck >= 10) {
        console.error('Unable to check files from leaseweb server after 10 attempts');
        break; // Skip batch after 10 attempts
      }
    }
  }
}

// Main execution
console.log('Starting log processing...');

// List all folders in bucket
console.log('Listing folders in bucket:', bucketName);
const folders = await listAllFolders(bucketName);
console.log(`Found ${folders.length} folders`);

// Randomize and sort folders
const shuffled = [...folders].sort(() => Math.random() - 0.5);
const sortedFolders = shuffled.sort((a, b) => {
  const dateA = a.split('/').slice(-2, -1)[0];
  const dateB = b.split('/').slice(-2, -1)[0];
  return dateA.localeCompare(dateB);
});

// Remove the last day
const foldersToProcess = sortedFolders.slice(0, -1);

// Get existing processed folders from database
const existingFolders: string[] = [];
const cmdCheck = "SELECT r2_path FROM `logs` WHERE `status` LIKE 'downloaded'";
const cmdExistingResults = await sqlQuery(cmdCheck, databaseGlobal, 'select') as any[];

for (const row of cmdExistingResults) {
  existingFolders.push(String(row.r2_path));
}

console.log('Existing folders:', existingFolders.length);

// Extract dates from existing folders (Python uses datetime.date() for comparison)
// We'll use string comparison which should work the same for YYYYMMDD format
const existingDates = new Set<string>();
for (const f of existingFolders) {
  // Python: datetime.strptime(f.split('/')[-1].split('.')[0], '%Y%m%d').date()
  const dateMatch = f.split('/').pop()?.split('.')[0];
  if (dateMatch && dateMatch.match(/^\d{8}$/)) {
    existingDates.add(dateMatch);
  }
}

// Filter out folders that are already processed
// Python: datetime.strptime(f.split('/')[-2], '%Y%m%d').date() not in existing_dates
const filteredFolders = foldersToProcess.filter(f => {
  const folderDate = f.split('/').slice(-2, -1)[0];
  return !existingDates.has(folderDate);
});

console.log('\nSorted List after Removing Existing Items:');
for (const folder of filteredFolders) {
  console.log(folder);
}

if (filteredFolders.length === 0) {
  console.log('\nNo items left after filtering.');
  process.exit(0);
}

// Process the first folder
const firstFolder = filteredFolders[0];
const dateDirectory = firstFolder.split('/').slice(-2, -1)[0];
console.log('\nFirst Item Date after Filtering:');
console.log(dateDirectory);

// Normalize paths - handle both absolute and relative paths
const logDir = config.directories.CLOUDFLARE_LOG_DIR.startsWith('./') 
  ? config.directories.CLOUDFLARE_LOG_DIR 
  : config.directories.CLOUDFLARE_LOG_DIR;
  
const directory = join(logDir, dateDirectory);
const outputDirectory = join(logDir, 'output');
const outputFilePath = join(logDir, 'allinone.txt');
const outputFilePathUnique = join(logDir, 'allinone_unique.txt');

// Step 1: Clean existing files
console.log('Step 1, cleaning existing files from previous iteration');
// Only remove the log directory if it exists, then recreate it
if (existsSync(logDir)) {
  removeDirectory(logDir);
}
ensureDirectoryExists(logDir);
ensureDirectoryExists(outputDirectory);

// Step 2: Download from source
console.log('Step 2, downloading from the source');
await downloadFolder(bucketName, dateDirectory, directory);

// Step 2: Analyze files (Python labels this as "Step 2" but it's actually step 3)
console.log('Step 2.5, analyzing the files');
const files = readdirSync(directory)
  .filter(f => f.endsWith('.log.gz'))
  .sort();

for (const fileName of files) {
  const filePath = join(directory, fileName);
  console.log(`Processing: ${filePath}`);
  
  const entryFetch = await parseLargeJsonFile(filePath);
  // Python: temp_out_file = output_directory + file_path.split('/')[-1]
  // This gets just the filename from the full path
  const tempOutFile = join(outputDirectory, fileName);

  console.log(tempOutFile);
  const outputLines: string[] = [];
  for (const entry of entryFetch) {
    // Python prints the entry before writing
    console.log(entry);
    const jsonString = JSON.stringify(entry);
    outputLines.push(jsonString);
  }
  writeFileSync(tempOutFile, outputLines.join('\n') + '\n');

  // Remove duplicates
  removeDuplicatesFile(tempOutFile);
}

// Step 3: Merge all files
console.log('Step 3, putting all files into one');
const outputLines: string[] = [];
for (const fileName of files) {
  const tempOutFile = join(outputDirectory, fileName);
  if (existsSync(tempOutFile)) {
    const content = readFileSync(tempOutFile, 'utf-8');
    const lines = content.split('\n').filter(line => line.trim());
    outputLines.push(...lines);
  }
}
writeFileSync(outputFilePath, outputLines.join('\n') + '\n');

// Step 4: Sort and deduplicate
console.log('Step 4, putting into the cloud and cleaning');
const allLines = readFileSync(outputFilePath, 'utf-8').split('\n').filter(line => line.trim());
const uniqueLines = Array.from(new Set(allLines)).sort();
writeFileSync(outputFilePathUnique, uniqueLines.join('\n') + '\n');

// Check if file exists and upload
if (existsSync(outputFilePathUnique)) {
  const rcloneResult = await rcloneCopy(outputFilePathUnique, `s3://${bucketNameClean}/${dateDirectory}.txt`);
  
  if (rcloneResult !== 'success') {
    console.error('Rclone copy failed');
    process.exit(1);
  }

  console.log('Copy Success, now delete');
  console.log('File exists, push it and remove all the garbage');
  removeDirectory(outputDirectory);
  removeDirectory(directory);
  removeFile(outputFilePath);
  removeFile(outputFilePathUnique);
} else {
  console.error('The user interaction could not be generated this is a serious issue');
  process.exit(1);
}

// Step 5: Update database
console.log('Step 5, updating mysql database');
const cmdUpdate = `INSERT INTO \`logs\` (\`day\`, \`status\`, \`r2_path\`) VALUES ('${dateDirectory}', 'downloaded', 's3://${bucketNameClean}/${dateDirectory}.txt')`;

const fileExists = await doesS3Exist(bucketNameClean, `${dateDirectory}.txt`);
if (fileExists) {
  await sqlQuery(cmdUpdate, databaseGlobal, 'update');
  console.log('Database updated successfully');
} else {
  console.error('file push failed, you cannot add it into the database');
  process.exit(1);
}

// Remove duplicates from not found file
removeDuplicatesFile(notFoundFile);

console.log('Processing complete!');

