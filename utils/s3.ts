import { S3Client, ListObjectsV2Command, PutObjectCommand, HeadObjectCommand, GetObjectCommand, CreateBucketCommand, HeadBucketCommand } from '@aws-sdk/client-s3';
import { config } from '../config.js';
import { execSync } from 'child_process';
import { readFileSync, mkdirSync, writeFileSync } from 'fs';
import { join, dirname } from 'path';
import dotenv from 'dotenv';

dotenv.config();

// Build S3 client configuration
const s3Config: any = {
  endpoint: config.s3.endpoint,
  region: config.s3.region
};

// If credentials are provided via environment variables, use them directly
if (process.env.AWS_ACCESS_KEY_ID && process.env.AWS_SECRET_ACCESS_KEY) {
  s3Config.credentials = {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
  };
} else {
  // Otherwise, set AWS_PROFILE so the SDK can use the credentials file
  // The default credential provider chain will look for ~/.aws/credentials
  if (config.s3.profile && !process.env.AWS_PROFILE) {
    process.env.AWS_PROFILE = config.s3.profile;
  }
}

// Create S3 client with credentials
// AWS SDK will use:
// 1. Explicit credentials from environment variables (if provided above)
// 2. AWS credentials file (~/.aws/credentials) with the profile from AWS_PROFILE
// 3. Default credential provider chain
const s3Client = new S3Client(s3Config);

// Create R2 client (for checking file existence after upload, same as log-views uses)
const r2AccessKeyId = process.env.R2_ACCESS_KEY_ID;
const r2SecretAccessKey = process.env.R2_SECRET_ACCESS_KEY;
const r2Endpoint = process.env.R2_ENDPOINT;
const r2Region = process.env.R2_REGION || 'auto';

let r2Client: S3Client | null = null;
if (r2AccessKeyId && r2SecretAccessKey && r2Endpoint) {
  r2Client = new S3Client({
    region: r2Region,
    endpoint: r2Endpoint,
    credentials: {
      accessKeyId: r2AccessKeyId,
      secretAccessKey: r2SecretAccessKey,
    },
    forcePathStyle: true, // Required for R2 (same as log-views)
  });
  console.log(`[R2_CLIENT] R2 client initialized with endpoint: ${r2Endpoint}`);
} else {
  console.warn(`[R2_CLIENT] R2 client not initialized - missing credentials. Check R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_ENDPOINT`);
}

export async function listAllFolders(bucket: string): Promise<string[]> {
  const folders: string[] = [];
  let continuationToken: string | undefined = undefined;

  do {
    const command: ListObjectsV2Command = new ListObjectsV2Command({
      Bucket: bucket,
      Delimiter: '/',
      ContinuationToken: continuationToken
    });

    const response = await s3Client.send(command) as any;

    if (response.CommonPrefixes) {
      for (const prefix of response.CommonPrefixes) {
        if (prefix.Prefix) {
          const folderPath = `s3://${bucket}/${prefix.Prefix}`;
          folders.push(folderPath);
        }
      }
    }

    continuationToken = response.NextContinuationToken;
  } while (continuationToken);

  return folders;
}

async function downloadSingleFile(bucket: string, key: string, localPath: string, prefix: string): Promise<void> {
  const getCommand = new GetObjectCommand({
    Bucket: bucket,
    Key: key
  });
  
  const getResponse = await s3Client.send(getCommand);
  
  // Calculate relative path from the folder prefix
  const relativePath = key.replace(prefix, '');
  const filePath = join(localPath, relativePath);
  const fileDir = dirname(filePath);
  
  // Ensure subdirectory exists
  if (fileDir !== localPath && fileDir !== '.') {
    mkdirSync(fileDir, { recursive: true });
  }
  
  // Read the stream and write to file
  if (getResponse.Body) {
    const chunks: Uint8Array[] = [];
    // @ts-ignore - Body is a Readable stream
    for await (const chunk of getResponse.Body) {
      chunks.push(chunk);
    }
    const buffer = Buffer.concat(chunks);
    writeFileSync(filePath, buffer);
  }
}

export async function downloadFolder(bucket: string, folderPath: string, localPath: string): Promise<boolean> {
  // Use AWS SDK to download files recursively with parallel batch downloads
  console.log(`Downloading s3://${bucket}/${folderPath} to ${localPath}`);
  
  try {
    // Ensure local directory exists
    mkdirSync(localPath, { recursive: true });
    
    // Normalize folder path (ensure it ends with /)
    const prefix = folderPath.endsWith('/') ? folderPath : folderPath + '/';
    
    // First, collect all file keys
    const fileKeys: string[] = [];
    let continuationToken: string | undefined = undefined;
    
    console.log('Listing files in S3...');
    do {
      const listCommand = new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        ContinuationToken: continuationToken
      });
      
      const listResponse = await s3Client.send(listCommand) as any;
      
      if (listResponse.Contents && listResponse.Contents.length > 0) {
        for (const object of listResponse.Contents) {
          if (object.Key) {
            fileKeys.push(object.Key);
          }
        }
      }
      
      continuationToken = listResponse.NextContinuationToken;
    } while (continuationToken);
    
    console.log(`Found ${fileKeys.length} files. Starting parallel downloads...`);
    
    // Download files in parallel batches
    const BATCH_SIZE = 20; // Download 20 files concurrently
    let totalDownloaded = 0;
    const startTime = Date.now();
    
    for (let i = 0; i < fileKeys.length; i += BATCH_SIZE) {
      const batch = fileKeys.slice(i, i + BATCH_SIZE);
      const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
      const totalBatches = Math.ceil(fileKeys.length / BATCH_SIZE);
      
      // Download batch in parallel
      await Promise.all(
        batch.map(async (key) => {
          try {
            await downloadSingleFile(bucket, key, localPath, prefix);
            totalDownloaded++;
            
            if (totalDownloaded % 50 === 0) {
              const elapsed = (Date.now() - startTime) / 1000;
              const rate = totalDownloaded / elapsed;
              const remaining = fileKeys.length - totalDownloaded;
              const eta = remaining / rate;
              console.log(`Progress: ${totalDownloaded}/${fileKeys.length} files (${rate.toFixed(1)} files/sec, ETA: ${eta.toFixed(0)}s)`);
            }
          } catch (error) {
            console.error(`Failed to download ${key}:`, error);
            throw error;
          }
        })
      );
      
      console.log(`Batch ${batchNumber}/${totalBatches} completed (${totalDownloaded}/${fileKeys.length} files)`);
    }
    
    const elapsed = (Date.now() - startTime) / 1000;
    const rate = totalDownloaded / elapsed;
    console.log(`Successfully downloaded ${totalDownloaded} files to ${localPath} in ${elapsed.toFixed(1)}s (${rate.toFixed(1)} files/sec)`);
    return true;
  } catch (error) {
    console.error('Download failed:', error);
    throw error;
  }
}

// Check if bucket exists, create if it doesn't
async function ensureBucketExists(bucket: string): Promise<void> {
  try {
    // Try to head the bucket (check if it exists)
    const headCommand = new HeadBucketCommand({ Bucket: bucket });
    await s3Client.send(headCommand);
    // Bucket exists, nothing to do
  } catch (error: any) {
    // If bucket doesn't exist (404), create it
    if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404 || error.Code === 'NoSuchBucket') {
      console.log(`Bucket '${bucket}' does not exist. Creating it...`);
      try {
        const createCommand = new CreateBucketCommand({
          Bucket: bucket
        });
        await s3Client.send(createCommand);
        console.log(`✓ Bucket '${bucket}' created successfully`);
      } catch (createError: any) {
        console.error(`Failed to create bucket '${bucket}':`, createError.message || createError);
        throw new Error(`Bucket '${bucket}' does not exist and could not be created. Please create it manually in your S3/Wasabi console.`);
      }
    } else {
      // Some other error, re-throw it
      throw error;
    }
  }
}

export async function uploadFile(localPath: string, bucket: string, key: string): Promise<boolean> {
  // Ensure bucket exists before uploading
  await ensureBucketExists(bucket);
  
  const fileContent = readFileSync(localPath);
  
  const command = new PutObjectCommand({
    Bucket: bucket,
    Key: key,
    Body: fileContent
  });

  try {
    await s3Client.send(command);
    return true;
  } catch (error: any) {
    if (error.Code === 'NoSuchBucket') {
      throw new Error(`Bucket '${bucket}' does not exist. Please create it in your S3/Wasabi console or check your bucket name configuration.`);
    }
    console.error('Upload failed:', error);
    throw error;
  }
}

export async function doesS3Exist(bucket: string, key: string): Promise<boolean> {
  // Check if this is an R2 bucket (clean-logs buckets are R2)
  const isR2Bucket = bucket.includes('clean-logs');
  
  if (isR2Bucket && r2Client) {
    // Use R2 client (same as log-views uses for downloading)
    // This ensures consistency between upload (rclone) and download (AWS SDK)
    try {
      console.log(`[VERIFY] Checking R2 for bucket="${bucket}", key="${key}"`);
      const command = new HeadObjectCommand({
        Bucket: bucket,
        Key: key
      });
      await r2Client.send(command);
      console.log(`[VERIFY] ✓ File exists in R2: ${bucket}/${key}`);
      return true;
    } catch (error: any) {
      if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404 || error.name === 'NoSuchKey') {
        console.log(`[VERIFY] ✗ File NOT found in R2: ${bucket}/${key}`);
        console.log(`[VERIFY]   Error: ${error.name || 'NotFound'}`);
        
        // Try to list files in the bucket to see what's actually there
        if (r2Client) {
          try {
            console.log(`[VERIFY] Listing files in bucket "${bucket}" to see what exists...`);
            const listCommand = new ListObjectsV2Command({
              Bucket: bucket,
              MaxKeys: 20
            });
            const listResponse = await r2Client.send(listCommand);
            if (listResponse.Contents && listResponse.Contents.length > 0) {
              console.log(`[VERIFY] Found ${listResponse.Contents.length} file(s) in bucket "${bucket}":`);
              listResponse.Contents.forEach(obj => {
                const isMatch = obj.Key === key;
                console.log(`[VERIFY]   ${isMatch ? '✓' : ' '} ${obj.Key} (size: ${obj.Size?.toLocaleString()} bytes, modified: ${obj.LastModified})`);
              });
              const exactMatch = listResponse.Contents.find(obj => obj.Key === key);
              if (!exactMatch) {
                console.log(`[VERIFY] ⚠ Expected key "${key}" not found in listing!`);
              }
            } else {
              console.log(`[VERIFY] ⚠ Bucket "${bucket}" appears to be empty!`);
            }
          } catch (listError: any) {
            console.warn(`[VERIFY] Could not list bucket contents:`, listError.message);
          }
        }
        
        return false;
      }
      throw error;
    }
  } else {
    // Use Wasabi S3 client (for source log downloads)
    try {
      const command = new HeadObjectCommand({
        Bucket: bucket,
        Key: key
      });
      await s3Client.send(command);
      return true;
    } catch (error: any) {
      if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
        return false;
      }
      throw error;
    }
  }
}

export async function rcloneCopy(localPath: string, remotePath: string): Promise<string> {
  // Check if remotePath is already in rclone format (r2:...) or s3:// format
  let rcloneRemotePath: string;
  
  if (remotePath.startsWith('r2:')) {
    // Already in rclone format: r2:bucket/path
    rcloneRemotePath = remotePath;
  } else if (remotePath.startsWith('s3://')) {
    // Convert s3:// to rclone format: s3://bucket/path -> r2:bucket/path
    // (assuming R2 is configured as 'r2:' remote in rclone)
    rcloneRemotePath = remotePath.replace(/^s3:\/\//, 'r2:');
  } else {
    // Assume it's just the path, prepend rclone remote
    rcloneRemotePath = `${config.rclone.remote}${remotePath}`;
  }
  
  // Always use rclone (like Python does)
  // Note: R2 credentials are configured in rclone config (~/.config/rclone/rclone.conf)
  // No need for AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY env vars for R2 uploads
  const command = `rclone copy "${localPath}" "${rcloneRemotePath}"`;
  console.log(`[RCLONE] ${command}`);
  
  try {
    execSync(command, { stdio: 'inherit' });
    return 'success';
  } catch (error) {
    console.error('Rclone copy failed:', error);
    console.error('Make sure rclone is configured with R2 credentials:');
    console.error('  Run: rclone config');
    console.error('  Create/edit remote named "r2:" with your Cloudflare R2 credentials');
    return 'failed';
  }
}

