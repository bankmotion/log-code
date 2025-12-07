import 'dotenv/config';
import { writeFileSync, existsSync, mkdirSync } from 'fs';
import { join, resolve } from 'path';
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
  // Use absolute path (like the main code does)
  const absoluteTestPath = resolve(testFilePath);
  console.log(`📤 Uploading to R2: r2:${bucketName}/${testDate}.txt`);
  console.log(`   Local file: ${absoluteTestPath}\n`);
  
  try {
    const result = await rcloneCopy(absoluteTestPath, `r2:${bucketName}/${testDate}.txt`);
    
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
    console.error('\n💡 Setup Instructions:');
    console.error('   1. Install rclone: sudo apt install rclone (or download from rclone.org)');
    console.error('   2. Configure R2 remote: rclone config');
    console.error('      - Create new remote named "r2"');
    console.error('      - Storage type: s3');
    console.error('      - Provider: Cloudflare');
    console.error('      - Enter your R2 Access Key ID, Secret Key, Endpoint, and Account ID');
    console.error('   3. Test manually: rclone ls r2:aznude-clean-logs');
    console.error('\n📖 See setup-rclone.md for detailed instructions');
    console.error('\n🔍 Quick check:');
    console.error('   - Config file location: ~/.config/rclone/rclone.conf');
    console.error('   - List remotes: rclone listremotes');
    console.error('   - Test connection: rclone lsd r2:');
  }
}

// Run the test
testUpload().catch(console.error);

