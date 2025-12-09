import 'dotenv/config';
import { readdirSync, writeFileSync, appendFileSync, existsSync, readFileSync, mkdirSync, createWriteStream, createReadStream } from 'fs';
import { createInterface } from 'readline';
import type { WriteStream } from 'fs';
import { join, resolve } from 'path';
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
import { whatIsGender, loadHtmlMap, identifyItem, clearDbQueryCache, preloadTableData, setMissingIdLogger, flushAllHtmlMapBatches } from './utils/urlIdentifier.js';

interface LogEntry {
  db?: string;
  id?: string;
  ip: string;
  date: string;
}

// Process a single gender
async function processGender(genderType: 'f' | 'm' | 'fans'): Promise<void> {
  const startTime = Date.now();
  
  // Store original console methods
  const originalLog = console.log.bind(console);
  const originalWarn = console.warn.bind(console);
  const originalError = console.error.bind(console);
  
  // Will be set up per date folder
  let currentLogStream: WriteStream | null = null;
  let currentLogFilePath: string | null = null;
  
  // Function to setup logging for a specific date
  function setupDateLogging(dateDirectory: string) {
    // Close previous log stream if exists
    if (currentLogStream !== null) {
      currentLogStream.end();
      currentLogStream = null;
    }
    
    // Setup logging for this date - save to logs/daily/{gender}/{date}/
    const dateLogDir = join('logs', 'daily', genderType, dateDirectory);
    ensureDirectoryExists(dateLogDir);
    
    currentLogFilePath = join(dateLogDir, 'processing.log');
    currentLogStream = createWriteStream(currentLogFilePath, { flags: 'a' }) as WriteStream;
    
    function writeToDateLog(level: string, ...args: any[]) {
      if (!currentLogStream) return;
      
      const timestamp = new Date().toISOString();
      const message = args.map(arg => 
        typeof arg === 'object' ? JSON.stringify(arg, null, 2) : String(arg)
      ).join(' ');
      const logLine = `[${timestamp}] [${level}] ${message}\n`;
      currentLogStream.write(logLine);
      
      // Also output to console
      if (level === 'LOG') originalLog(...args);
      else if (level === 'WARN') originalWarn(...args);
      else if (level === 'ERROR') originalError(...args);
    }
    
    console.log = (...args: any[]) => writeToDateLog('LOG', ...args);
    console.warn = (...args: any[]) => writeToDateLog('WARN', ...args);
    console.error = (...args: any[]) => writeToDateLog('ERROR', ...args);
  }
  
  console.log(`\n${'='.repeat(60)}`);
  console.log(`Processing gender: ${genderType}`);
  console.log(`${'='.repeat(60)}\n`);

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
    return;
  }

  // Use local logs folder for notfound file
  const notFoundFile = `./logs/notfound-${genderGlobalLog}.txt`;
  // Separate file for missing IDs (IDs extracted from HTML but not found in database)
  const missingIdFile = `./logs/missing-ids-${genderGlobalLog}.txt`;
  
  // Ensure logs directory exists
  ensureDirectoryExists('./logs');
  
  // Set up missing ID logger - logs URLs where ID was extracted but not found in database
  setMissingIdLogger((url: string, table: string, id: string, database: string) => {
    const logLine = `${url} | Table: ${table} | ID: ${id} | Database: ${database}\n`;
    appendFileSync(missingIdFile, logLine);
  });

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
  const BATCH_SIZE = 30; // Check 30 files per SSH command (reduced to prevent timeouts)

  // Determine if file is gzipped
  const isGzipped = filePath.endsWith('.gz');
  const fileIterator = isGzipped ? readGzippedFile(filePath) : readTextFile(filePath);

  let lineCount = 0;
  for await (const line of fileIterator) {
    lineCount++;

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

  console.log(`Processed ${lineCount} lines from ${filePath}`);

  // Process remaining batch items
  if (unidentifiedFiles.length > 0) {
    await processBatchSSHChecks(unidentifiedFiles, notFoundFile);
  }

  return entries;
}

// Batch process SSH file existence checks
// Track if SSH has been tested and failed
let sshAuthFailed = false;

async function processBatchSSHChecks(
  files: Array<{ host: string; path: string; serverPath: string }>,
  notFoundFile: string
): Promise<void> {
  if (files.length === 0) return;
  
  // Skip SSH checks if authentication has already failed
  if (sshAuthFailed) {
    return; // Silently skip - SSH is not available
  }
  
  // Calculate SSH timeout (60 seconds max - increased for large batches)
  // Use a longer timeout for large batches to prevent premature timeouts
  const sshTimeout = Math.min(60000, 1000 + (files.length * 1000)); // 1s base + 1s per file, max 60s
  
  // Log total file count before starting batch SSH process
  const sshBatchStartTime = Date.now();
  // console.log(`[SSH] Processing ${files.length} files (timeout: ${sshTimeout/1000}s)`);
  
  // Build a single SSH command that checks all files
  // Use a simpler approach: escape paths properly and construct the command
  // Structure: ssh ... "sh -c 'for file in ...; do ...; done'"
  const filePaths = files.map(f => f.serverPath);
  
  // Escape each path: replace ' with '\'' (end quote, escaped quote, start quote)
  // Then wrap in single quotes
  const escapedPaths = filePaths.map(path => {
    const escaped = path.replace(/'/g, "'\\''");
    return `'${escaped}'`;
  }).join(' ');
  
  // Build the for loop command
  // Inside single quotes, we need to escape: $ becomes \$ (for variables)
  const forLoop = `for file in ${escapedPaths}; do test -f "\\$file" && echo "EXISTS:\\$file" || echo "NOTEXISTS:\\$file"; done`;
  
  // Now wrap in single quotes for sh -c, escaping any single quotes in forLoop
  // Single quotes in forLoop need to become: ' becomes '\'' 
  const escapedForLoop = forLoop.replace(/'/g, "'\\''");
  
  // Final command: ssh ... "sh -c '...'"
  // Outer double quotes for SSH, inner single quotes for sh -c
  const command = `${config.LEASEWEB_SERVER_SSH} "sh -c '${escapedForLoop}'"`;
  
  let attemptsFileCheck = 0;
  let unexpectedError = 0;

  while (true) {
    try {
      const normalizedCommand = Buffer.from(command, 'utf-8').toString('utf-8');
      
      if (attemptsFileCheck > 0) {
        // console.log(`[SSH] Retry attempt ${attemptsFileCheck + 1}/10 for ${files.length} files`);
      }
      
      const sshCommandStartTime = Date.now();
      // console.log(`[SSH] Executing command (timeout: ${sshTimeout/1000}s)...`);
      
      const outputCommand = bashCommand(normalizedCommand, sshTimeout);
      
      const sshCommandDuration = ((Date.now() - sshCommandStartTime) / 1000).toFixed(2);
      // console.log(`[SSH] Command completed in ${sshCommandDuration}s`);
      
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
      
      const sshBatchDuration = ((Date.now() - sshBatchStartTime) / 1000).toFixed(2);
      // console.log(`[SSH] Batch completed in ${sshBatchDuration}s - Found: ${foundCount}/${files.length} files exist`);
      
      break; // Success
      
    } catch (error: any) {
      const errorMessage = error.message || String(error);
      const errorOutput = error.stderr || error.output?.[2] || '';
      
      // Check if it's a persistent SSH authentication error
      if (errorMessage.includes('Permission denied (publickey)') || 
          errorOutput.includes('Permission denied (publickey)')) {
        // Mark SSH as failed and only log once
        if (!sshAuthFailed) {
          sshAuthFailed = true;
          console.warn(`\n[SSH] ⚠️  Authentication failed - SSH file existence checks will be skipped`);
          console.warn(`[SSH] Reason: Permission denied (publickey)`);
          console.warn(`[SSH] Current config: ${config.LEASEWEB_SERVER_SSH}`);
          console.warn(`[SSH] To fix:`);
          console.warn(`[SSH]   1. Copy your private key to: ./key/id_rsa (in project root)`);
          console.warn(`[SSH]   2. Update .env: LEASEWEB_SERVER_SSH="ssh -i ./key/id_rsa pavel@67.205.170.17"`);
          console.warn(`[SSH]   3. On Linux/Mac: chmod 600 ./key/id_rsa (Windows doesn't need this)\n`);
        }
        break; // Skip batch, continue processing
      }
      
      // Check if it's a timeout error
      if (errorMessage.includes('timeout') || errorMessage.includes('ETIMEDOUT') || errorMessage.includes('timed out')) {
        console.error(`[SSH] Timeout after ${sshTimeout/1000}s (attempt ${attemptsFileCheck + 1}/10)`);
      } else {
        console.error(`[SSH] Batch check failed (attempt ${attemptsFileCheck + 1}):`, errorMessage.substring(0, 200));
      }
      
      unexpectedError++;
      await new Promise(resolve => setTimeout(resolve, 10000));
      
      if (unexpectedError >= 10) {
        console.error(`[SSH] Batch check failed 10 times - skipping ${files.length} files`);
        break; // Skip batch after 10 failures
      }
      
      attemptsFileCheck++;
      if (attemptsFileCheck >= 10) {
        console.error(`[SSH] Unable to check files after 10 attempts - skipping ${files.length} files`);
        break; // Skip batch after 10 attempts
      }
    }
  }
}

  // List all folders in bucket
  console.log(`[${genderType}] Listing folders in bucket: ${bucketName}`);
  const folders = await listAllFolders(bucketName);
  console.log(`[${genderType}] Found ${folders.length} total folders in bucket`);

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

  console.log(`[${genderType}] Existing processed folders in database: ${existingFolders.length}`);

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

  console.log(`[${genderType}] Folders to process after filtering: ${filteredFolders.length}`);
  
  if (filteredFolders.length > 0) {
    console.log(`[${genderType}] Folders to process:`);
    filteredFolders.forEach((folder, index) => {
      const date = folder.split('/').slice(-2, -1)[0];
      console.log(`  ${index + 1}. ${date} (${folder})`);
    });
  }

  if (filteredFolders.length === 0) {
    console.log(`[${genderType}] No items left for processing. Skipping gender ${genderType}.`);
    return;
  }

  // Process ALL folders (not just the first one)
  let processedCount = 0;
  let successCount = 0;
  let failedCount = 0;
  
  for (const folder of filteredFolders) {
    const dateDirectory = folder.split('/').slice(-2, -1)[0];
    processedCount++;
    
    // Setup logging for this date folder
    setupDateLogging(dateDirectory);
    
    // Clear and pre-load database tables at the start of each date processing
    // This loads all celebs, movies, stories into memory for fast lookups
    clearDbQueryCache();
    await preloadTableData(databaseGlobal);
    
    // Clear missing ID file at the start of each date (optional - comment out if you want cumulative log)
    // writeFileSync(missingIdFile, ''); // Uncomment to clear file per date
    
    console.log(`\n${'='.repeat(60)}`);
    console.log(`[${genderType}] Processing folder ${processedCount}/${filteredFolders.length}: ${dateDirectory}`);
    console.log(`${'='.repeat(60)}`);
    console.log(`📝 Logging to: ${currentLogFilePath}\n`);
    
    const folderStartTime = Date.now();

    // Normalize paths - handle both absolute and relative paths
    const logDir = config.directories.CLOUDFLARE_LOG_DIR.startsWith('./') 
      ? config.directories.CLOUDFLARE_LOG_DIR 
      : config.directories.CLOUDFLARE_LOG_DIR;
      
    const directory = join(logDir, dateDirectory);
    const outputDirectory = join(logDir, 'output');
    const outputFilePath = join(logDir, 'allinone.txt');
    const outputFilePathUnique = join(logDir, 'allinone_unique.txt');

    // Step 1: Clean existing files
    console.log(`[${genderType}][${dateDirectory}] Step 1: Cleaning existing files from previous iteration`);
    // Only remove the log directory if it exists, then recreate it
    if (existsSync(logDir)) {
      removeDirectory(logDir);
    }
    ensureDirectoryExists(logDir);
    ensureDirectoryExists(outputDirectory);

    // Step 2: Download from source
    console.log(`[${genderType}][${dateDirectory}] Step 2: Downloading from source (bucket: ${bucketName})`);
    await downloadFolder(bucketName, dateDirectory, directory);
    console.log(`[${genderType}][${dateDirectory}] Download completed`);

    // Step 2: Analyze files (Python labels this as "Step 2" but it's actually step 3)
    console.log(`[${genderType}][${dateDirectory}] Step 2.5: Analyzing files`);
    const files = readdirSync(directory)
      .filter(f => f.endsWith('.log.gz'))
      .sort();
    console.log(`[${genderType}][${dateDirectory}] Found ${files.length} log files to process`);

    // Process files in smaller batches with temp files
    const FILE_BATCH_SIZE = 100;
    let totalProcessed = 0;
    let totalErrors = 0;

    for (let i = 0; i < files.length; i += FILE_BATCH_SIZE) {
      const batch = files.slice(i, i + FILE_BATCH_SIZE);
      const batchNumber = Math.floor(i / FILE_BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(files.length / FILE_BATCH_SIZE);
      
      console.log(`[${genderType}][${dateDirectory}] Processing batch ${batchNumber}/${totalBatches} (${batch.length} files)...`);
      
      const batchStartTime = Date.now();
      
      // Process batch in parallel with timeout protection
      const batchPromises = batch.map(async (fileName) => {
        const fileStartTime = Date.now();
        const fileTimeout = 30 * 60 * 1000; // 30 minutes per file
        
        try {
          const filePath = join(directory, fileName);
          console.log(`[FILE] Starting: ${fileName} (timeout: ${fileTimeout/1000/60} minutes)`);
          
          // Add timeout wrapper for file processing
          const processPromise = parseLargeJsonFile(filePath);
          const timeoutPromise = new Promise<never>((_, reject) => 
            setTimeout(() => {
              const elapsed = ((Date.now() - fileStartTime) / 1000 / 60).toFixed(1);
              reject(new Error(`Timeout: File processing exceeded ${fileTimeout/1000/60} minutes (${elapsed} minutes elapsed)`));
            }, fileTimeout)
          );
          
          const entryFetch = await Promise.race([processPromise, timeoutPromise]);
          
          const fileDuration = ((Date.now() - fileStartTime) / 1000).toFixed(1);
          console.log(`[FILE] Completed: ${fileName} in ${fileDuration}s (${entryFetch.length} entries)`);
          
          // Python: temp_out_file = output_directory + file_path.split('/')[-1]
          // This gets just the filename from the full path
          const tempOutFile = join(outputDirectory, fileName);

          const outputLines: string[] = [];
          for (const entry of entryFetch) {
            // Python prints the entry before writing, but we skip it for cleaner logs
            const jsonString = JSON.stringify(entry);
            outputLines.push(jsonString);
          }
          writeFileSync(tempOutFile, outputLines.join('\n') + '\n');

          // Remove duplicates
          removeDuplicatesFile(tempOutFile);
          
          return { fileName, success: true, entryCount: entryFetch.length };
        } catch (error: any) {
          const fileDuration = ((Date.now() - fileStartTime) / 1000).toFixed(1);
          const errorMsg = error.message || String(error);
          console.error(`[FILE] Failed: ${fileName} after ${fileDuration}s - ${errorMsg}`);
          console.error(`[${genderType}][${dateDirectory}] Error processing file ${fileName}:`, errorMsg);
          
          return { fileName, success: false, error: errorMsg, entryCount: 0 };
        }
      });

      // Wait for all files in batch to complete in parallel with overall timeout
      // Use a shorter timeout (30 minutes) - don't wait for stuck files
      const batchTimeout = 30 * 60 * 1000; // 30 minutes max for batch
      let batchResults: Array<{ fileName: string; success: boolean; error?: string; entryCount: number }>;
      
      console.log(`[BATCH] Waiting for ${batch.length} files to complete (timeout: 30 minutes, will proceed with completed files)...`);
      
      // Track progress with periodic updates and identify stuck files
      let completedCount = 0;
      const fileStartTimes = new Map<string, number>();
      batch.forEach(fileName => fileStartTimes.set(fileName, Date.now()));
      
      const progressInterval = setInterval(() => {
        const elapsed = ((Date.now() - batchStartTime) / 1000 / 60).toFixed(1);
        const pendingFiles = batch.filter(f => !completionTracker.has(f));
        const stuckFiles = pendingFiles.filter(f => {
          const startTime = fileStartTimes.get(f) || batchStartTime;
          return (Date.now() - startTime) > 20 * 60 * 1000; // Files running > 20 minutes
        });
        
        console.log(`[BATCH] Progress: ${completedCount}/${batch.length} files completed, ${elapsed} minutes elapsed`);
        if (stuckFiles.length > 0) {
          console.log(`[BATCH] ⚠️  ${stuckFiles.length} files appear stuck (>20 min): ${stuckFiles.slice(0, 5).join(', ')}${stuckFiles.length > 5 ? '...' : ''}`);
        }
      }, 60000); // Log every minute
      
      // Track completion for each file
      const completionTracker = new Map<string, { fileName: string; success: boolean; error?: string; entryCount: number }>();
      
      // Wrap each promise to track completion
      const trackedPromises = batchPromises.map((promise, index) => 
        promise.then(result => {
          completedCount++;
          completionTracker.set(batch[index], result);
          return result;
        }).catch(error => {
          completedCount++;
          const errorResult = {
            fileName: batch[index],
            success: false,
            error: error.message || String(error),
            entryCount: 0
          };
          completionTracker.set(batch[index], errorResult);
          return errorResult;
        })
      );
      
      try {
        // Use a shorter timeout (30 minutes) and don't wait for all files
        const batchTimeout = 30 * 60 * 1000; // 30 minutes max for batch
        const batchTimeoutPromise = new Promise<void>((resolve) => 
          setTimeout(() => {
            console.log(`[BATCH] Batch timeout reached - proceeding with completed files`);
            resolve();
          }, batchTimeout)
        );
        
        // Race between all files completing and timeout
        await Promise.race([
          Promise.all(trackedPromises),
          batchTimeoutPromise
        ]);
        
        clearInterval(progressInterval);
        
        // Build results from completion tracker (use whatever we have)
        batchResults = batch.map(fileName => 
          completionTracker.get(fileName) || {
            fileName,
            success: false,
            error: 'File did not complete within batch timeout',
            entryCount: 0
          }
        );
        
        const finalCompleted = batchResults.filter(r => r.success).length;
        console.log(`[BATCH] Batch ${batchNumber} finished: ${finalCompleted}/${batch.length} files succeeded`);
      } catch (error: any) {
        clearInterval(progressInterval);
        
        if (error.message && error.message.includes('Batch processing exceeded')) {
          const elapsed = ((Date.now() - batchStartTime) / 1000 / 60).toFixed(1);
          console.error(`[BATCH] Batch ${batchNumber} timed out after ${elapsed} minutes`);
          console.error(`[BATCH] Completed: ${completedCount}/${batch.length} files before timeout`);
          
          // Identify which files are still pending
          const pendingFiles = batch.filter(f => !completionTracker.has(f));
          if (pendingFiles.length > 0) {
            console.error(`[BATCH] ⚠️  ${pendingFiles.length} files did not complete:`);
            pendingFiles.forEach(fileName => {
              const startTime = fileStartTimes.get(fileName) || batchStartTime;
              const fileElapsed = ((Date.now() - startTime) / 1000 / 60).toFixed(1);
              console.error(`[BATCH]   - ${fileName} (running for ${fileElapsed} minutes)`);
            });
          }
          
          // Use whatever results we have from the tracker
          batchResults = batch.map(fileName => 
            completionTracker.get(fileName) || {
              fileName,
              success: false,
              error: 'Batch timeout - processing exceeded 2 hours',
              entryCount: 0
            }
          );
        } else {
          throw error;
        }
      }
      
      // Count successes and errors
      const batchSuccesses = batchResults.filter(r => r.success).length;
      const batchErrors = batchResults.filter(r => !r.success).length;
      const batchEntryCount = batchResults.reduce((sum, r) => sum + (r.entryCount || 0), 0);
      
      totalProcessed += batchSuccesses;
      totalErrors += batchErrors;
      
      const batchDuration = ((Date.now() - batchStartTime) / 1000).toFixed(1);
      const elapsedTotal = ((Date.now() - folderStartTime) / 1000).toFixed(1);
      const avgTimePerFile = totalProcessed > 0 ? (Date.now() - folderStartTime) / totalProcessed : 0;
      const remainingFiles = files.length - totalProcessed;
      const etaSeconds = remainingFiles * avgTimePerFile / 1000;
      const etaMinutes = Math.floor(etaSeconds / 60);
      
      // Flush html_map inserts after each batch to avoid memory buildup
      const flushStartTime = Date.now();
      console.log(`[DB] Flushing html_map batch inserts...`);
      await flushAllHtmlMapBatches();
      const flushDuration = ((Date.now() - flushStartTime) / 1000).toFixed(2);
      console.log(`[DB] Batch flush completed in ${flushDuration}s`);
      
      // Log batch summary with record counts
      console.log(`[${genderType}][${dateDirectory}] Batch ${batchNumber}/${totalBatches} completed in ${batchDuration}s`);
      console.log(`[${genderType}][${dateDirectory}]   Success: ${batchSuccesses}, Errors: ${batchErrors}, Entries: ${batchEntryCount.toLocaleString()}`);
      console.log(`[${genderType}][${dateDirectory}]   Total progress: ${totalProcessed}/${files.length} files (${((totalProcessed/files.length)*100).toFixed(1)}%)`);
      if (etaMinutes > 0) {
        console.log(`[${genderType}][${dateDirectory}]   Elapsed: ${elapsedTotal}s, ETA: ${etaMinutes}m`);
      }
      
      // Log detailed record counts per file (only for successful files)
      if (batchEntryCount > 0) {
        const successfulFiles = batchResults.filter(r => r.success && r.entryCount > 0);
        if (successfulFiles.length > 0) {
          console.log(`  ✓ Record breakdown:`);
          successfulFiles.forEach(r => {
            console.log(`    - ${r.fileName}: ${r.entryCount.toLocaleString()} records`);
          });
        }
      }
      
      // Log any errors
      if (batchErrors > 0) {
        console.log(`  ✗ Failed files:`);
        const failedFiles = batchResults.filter(r => !r.success);
        failedFiles.forEach(r => {
          console.error(`    - ${r.fileName}: ${r.error}`);
        });
      }
    }
    
    // Calculate total records across all batches
    let totalRecords = 0;
    for (const fileName of files) {
      const tempOutFile = join(outputDirectory, fileName);
      if (existsSync(tempOutFile)) {
        try {
          const content = readFileSync(tempOutFile, 'utf-8');
          const lines = content.split('\n').filter(line => line.trim());
          totalRecords += lines.length;
        } catch (error) {
          // Skip if file can't be read
        }
      }
    }
    
    console.log(`\n[${genderType}][${dateDirectory}] 📊 File Processing Summary:`);
    console.log(`  ✓ Files processed: ${totalProcessed}/${files.length} successful`);
    console.log(`  ✗ Files failed: ${totalErrors}`);
    console.log(`  📝 Total records created: ${totalRecords.toLocaleString()}`);

    // Step 3: Merge all files (using streaming to avoid memory issues)
    console.log(`[${genderType}][${dateDirectory}] Step 3: Merging all files into one (streaming mode)`);
    
    // Create output file stream
    const outputStream = createWriteStream(outputFilePath, { flags: 'w' });
    let mergedCount = 0;
    
    // Process files one at a time and append to output
    for (const fileName of files) {
      const tempOutFile = join(outputDirectory, fileName);
      if (existsSync(tempOutFile)) {
        try {
          // Read file line by line and write to output stream
          const fileStream = createReadStream(tempOutFile, { encoding: 'utf-8' });
          const rl = createInterface({
            input: fileStream,
            crlfDelay: Infinity
          });
          
          for await (const line of rl) {
            const trimmed = line.trim();
            if (trimmed) {
              outputStream.write(trimmed + '\n');
            }
          }
          
          mergedCount++;
          if (mergedCount % 100 === 0) {
            console.log(`[${genderType}][${dateDirectory}] Merged ${mergedCount}/${files.length} files...`);
          }
        } catch (error: any) {
          console.error(`[${genderType}][${dateDirectory}] Error merging file ${fileName}:`, error.message || error);
        }
      }
    }
    
    // Close the output stream
    outputStream.end();
    await new Promise<void>((resolve) => {
      outputStream.on('close', () => resolve());
    });
    
    console.log(`[${genderType}][${dateDirectory}] ✓ Merged ${mergedCount} files successfully`);

    // Step 4: Sort and deduplicate using system sort (memory efficient)
    // Use 'sort -u' which handles both sorting and deduplication efficiently
    // This uses external sorting and doesn't load everything into memory
    console.log(`[${genderType}][${dateDirectory}] Step 4: Sorting and deduplicating (using system sort -u)`);
    
    const { execSync } = await import('child_process');
    
    try {
      // Use system 'sort -u' command:
      // -u: unique (remove duplicates)
      // This uses external sorting and is very memory efficient
      console.log(`[${genderType}][${dateDirectory}] Running system sort -u (this may take a while for large files)...`);
      
      execSync(`sort -u "${outputFilePath}" -o "${outputFilePathUnique}"`, { 
        encoding: 'utf-8',
        maxBuffer: 1024 * 1024 * 1024, // 1GB buffer
        stdio: 'inherit' // Show progress
      });
      
      console.log(`[${genderType}][${dateDirectory}] ✓ Sort and deduplication complete`);
      
      // Count lines in the final file
      const finalStream = createReadStream(outputFilePathUnique, { encoding: 'utf-8' });
      const finalRl = createInterface({
        input: finalStream,
        crlfDelay: Infinity
      });
      
      let finalCount = 0;
      for await (const line of finalRl) {
        if (line.trim()) {
          finalCount++;
        }
      }
      
      console.log(`[${genderType}][${dateDirectory}] ✓ Final file contains ${finalCount.toLocaleString()} unique lines`);
    } catch (error: any) {
      console.error(`[${genderType}][${dateDirectory}] System sort failed:`, error.message);
      console.error(`[${genderType}][${dateDirectory}] Attempting fallback method...`);
      
      // Fallback: Use streaming with smaller chunks
      // This is less memory efficient but should work if system sort is unavailable
      console.log(`[${genderType}][${dateDirectory}] Using streaming deduplication fallback...`);
      
      const seen = new Set<string>();
      const uniqueStream = createWriteStream(outputFilePathUnique, { flags: 'w' });
      const inputStream = createReadStream(outputFilePath, { encoding: 'utf-8' });
      const rl = createInterface({
        input: inputStream,
        crlfDelay: Infinity
      });
      
      let uniqueCount = 0;
      const MAX_SET_SIZE = 5000000; // Limit Set size to 5M entries
      
      for await (const line of rl) {
        const trimmed = line.trim();
        if (trimmed) {
          if (!seen.has(trimmed)) {
            seen.add(trimmed);
            uniqueStream.write(trimmed + '\n');
            uniqueCount++;
            
            // Periodically clear the Set to free memory (we've already written unique lines)
            if (seen.size >= MAX_SET_SIZE) {
              seen.clear();
              if (uniqueCount % 1000000 === 0) {
                console.log(`[${genderType}][${dateDirectory}] Processed ${uniqueCount.toLocaleString()} unique lines...`);
              }
            }
            
            if (uniqueCount % 1000000 === 0) {
              console.log(`[${genderType}][${dateDirectory}] Deduplicated ${uniqueCount.toLocaleString()} unique lines...`);
            }
          }
        }
      }
      
      uniqueStream.end();
      await new Promise<void>((resolve) => {
        uniqueStream.on('close', () => resolve());
      });
      
      console.log(`[${genderType}][${dateDirectory}] ✓ Fallback deduplication complete: ${uniqueCount.toLocaleString()} unique lines`);
      
      // Now sort the deduplicated file
      console.log(`[${genderType}][${dateDirectory}] Sorting deduplicated file...`);
      try {
        execSync(`sort "${outputFilePathUnique}" -o "${outputFilePathUnique}"`, { 
          encoding: 'utf-8',
          maxBuffer: 1024 * 1024 * 1024
        });
        console.log(`[${genderType}][${dateDirectory}] ✓ Sorting complete`);
      } catch (sortError: any) {
        console.error(`[${genderType}][${dateDirectory}] Sort failed:`, sortError.message);
        throw sortError;
      }
    }

    // Check if file exists and upload
    if (existsSync(outputFilePathUnique)) {
      // Use rclone to upload to R2 (like Python does)
      // Format: r2:bucket-name/path/to/file.txt
      // Convert to absolute path for rclone (Python uses absolute paths like /var/www/cloudflarelog/...)
      const absolutePath = resolve(outputFilePathUnique);
      const remotePath = `r2:${bucketNameClean}/${dateDirectory}.txt`;
      const rcloneResult = await rcloneCopy(absolutePath, remotePath);
      
      if (rcloneResult !== 'success') {
        console.error('Rclone copy failed');
        process.exit(1);
      }


      console.log(`[${genderType}][${dateDirectory}] Upload successful! Cleaning up local files...`);
      removeDirectory(outputDirectory);
      removeDirectory(directory);
      removeFile(outputFilePath);
      removeFile(outputFilePathUnique);
    } else {
      console.error('The user interaction could not be generated this is a serious issue');
      process.exit(1);
    }

    // Flush any remaining html_map inserts before updating logs table
    console.log(`[${genderType}][${dateDirectory}] Flushing remaining html_map inserts...`);
    await flushAllHtmlMapBatches();

    // Step 5: Update database
    console.log(`[${genderType}][${dateDirectory}] Step 5: Updating MySQL database`);
    const cmdUpdate = `INSERT INTO \`logs\` (\`day\`, \`status\`, \`r2_path\`) VALUES ('${dateDirectory}', 'downloaded', 's3://${bucketNameClean}/${dateDirectory}.txt')`;

    const fileExists = await doesS3Exist(bucketNameClean, `${dateDirectory}.txt`);
    if (fileExists) {
      await sqlQuery(cmdUpdate, databaseGlobal, 'update');
      console.log(`[${genderType}][${dateDirectory}] Database updated successfully`);
      successCount++;
    } else {
      console.error(`[${genderType}][${dateDirectory}] ERROR: File push failed, cannot add to database`);
      failedCount++;
      process.exit(1);
    }

    // Remove duplicates from not found file
    removeDuplicatesFile(notFoundFile);

    const folderEndTime = Date.now();
    const folderDuration = ((folderEndTime - folderStartTime) / 1000).toFixed(1);
    console.log(`[${genderType}][${dateDirectory}] ✓ Completed in ${folderDuration}s`);
  }

  const endTime = Date.now();
  const totalDuration = ((endTime - startTime) / 1000).toFixed(1);
  console.log(`\n${'='.repeat(60)}`);
  console.log(`[${genderType}] Summary:`);
  console.log(`  Total folders processed: ${processedCount}`);
  console.log(`  Successful: ${successCount}`);
  console.log(`  Failed: ${failedCount}`);
  console.log(`  Total time: ${totalDuration}s`);
  console.log(`${'='.repeat(60)}\n`);
  
  // Close current log stream and restore original console
  if (currentLogStream) {
    (currentLogStream as WriteStream).end();
    currentLogStream = null;
  }
  console.log = originalLog;
  console.warn = originalWarn;
  console.error = originalError;
  
  // Clear missing ID logger
  setMissingIdLogger(null);
  
  // Flush any remaining html_map inserts
  await flushAllHtmlMapBatches();
}

// Main execution
const mainStartTime = Date.now();
console.log('\n' + '='.repeat(60));
console.log('🚀 Starting log processing system');
console.log('='.repeat(60));

// Load HTML map associations (shared across all genders)
console.log('\n📋 Loading HTML map associations...');
await loadHtmlMap();
console.log('✓ HTML map loaded successfully\n');

// Process all genders sequentially: f, then m, then fans
// Currently disabled: 'm' and 'fans' - only processing 'f'
const genders: Array<'f' | 'm' | 'fans'> = ['f'];
const genderResults: Array<{ gender: string; success: boolean; error?: string }> = [];

for (const gender of genders) {
  try {
    await processGender(gender);
    genderResults.push({ gender, success: true });
  } catch (error: any) {
    console.error(`\n❌ Error processing gender ${gender}:`, error.message || error);
    genderResults.push({ gender, success: false, error: error.message || String(error) });
    // Continue with next gender even if one fails
  }
}

const mainEndTime = Date.now();
const mainDuration = ((mainEndTime - mainStartTime) / 1000).toFixed(1);

console.log('\n' + '='.repeat(60));
console.log('📊 FINAL SUMMARY');
console.log('='.repeat(60));
console.log(`Total execution time: ${mainDuration}s`);
console.log('\nGender processing results:');
genderResults.forEach(result => {
  const status = result.success ? '✓' : '✗';
  console.log(`  ${status} ${result.gender}${result.error ? ` (Error: ${result.error})` : ''}`);
});
console.log('='.repeat(60));
console.log('✨ All processing complete!\n');
