import 'dotenv/config';
import { writeFileSync, existsSync, mkdirSync } from 'fs';
import { join } from 'path';
import { rcloneCopy } from './utils/s3.js';

// Test script to upload a file to R2 (aznude-clean-logs bucket)
async function testUpload() {
  console.log('🧪 Testing R2 upload...\n');

  // Test date: 2025.11.17 (format: 20251117)
  const testDate = '20251117';
  const bucketName = 'aznude-clean-logs';
  
  // Create test file
  const testDir = './logs/test';
  if (!existsSync(testDir)) {
    mkdirSync(testDir, { recursive: true });
  }
  
  const testFileName = `test-${testDate}.txt`;
  const testFilePath = join(testDir, testFileName);
  
  // Create test content
  const testContent = `Test file for date: ${testDate}
Created at: ${new Date().toISOString()}
This is a test upload to R2 bucket: ${bucketName}

Test data:
- Line 1
- Line 2
- Line 3
- Line 4
- Line 5
`;
  
  console.log(`📝 Creating test file: ${testFilePath}`);
  writeFileSync(testFilePath, testContent);
  console.log(`✓ Test file created (${testContent.split('\n').length} lines)\n`);
  
  // Test upload to R2
  console.log(`📤 Uploading to R2: r2:${bucketName}/${testDate}.txt`);
  console.log(`   Local file: ${testFilePath}\n`);
  
  try {
    const result = await rcloneCopy(testFilePath, `r2:${bucketName}/${testDate}.txt`);
    
    if (result === 'success') {
      console.log('\n✅ Upload successful!');
      console.log(`   File uploaded to: r2:${bucketName}/${testDate}.txt`);
      console.log(`\n💡 You can verify the upload by running:`);
      console.log(`   rclone ls r2:${bucketName}/`);
    } else {
      console.error('\n❌ Upload failed!');
      console.error('   Check rclone configuration and R2 credentials');
    }
  } catch (error: any) {
    console.error('\n❌ Upload error:', error.message || error);
    console.error('\n💡 Make sure:');
    console.error('   1. Rclone is installed: rclone --version');
    console.error('   2. Rclone is configured: rclone config');
    console.error('   3. Remote "r2:" exists in rclone config');
    console.error('   4. R2 credentials are correct');
  }
}

// Run the test
testUpload().catch(console.error);

