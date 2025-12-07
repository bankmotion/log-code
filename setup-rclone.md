# Rclone Setup for Cloudflare R2

## Step 1: Install Rclone (if not installed)

```bash
# Check if rclone is installed
rclone --version

# If not installed, install it:
# Ubuntu/Debian:
sudo apt install rclone

# Or download from: https://rclone.org/downloads/
```

## Step 2: Configure R2 Remote

Run the interactive config:

```bash
rclone config
```

Follow these steps:

1. **Press `n`** to create a new remote
2. **Enter name**: `r2` (this will be your remote name)
3. **Storage type**: Type `s3` and press Enter
4. **Provider**: Type `Cloudflare` and press Enter
5. **Access Key ID**: Enter your R2 Access Key ID
6. **Secret Access Key**: Enter your R2 Secret Access Key
7. **Endpoint**: Enter your R2 endpoint URL (e.g., `https://xxxxx.r2.cloudflarestorage.com`)
8. **Account ID**: Enter your Cloudflare Account ID
9. **Region**: Press Enter (leave default)
10. **Location constraint**: Press Enter (leave default)
11. **ACL**: Press Enter (leave default)
12. **Storage class**: Press Enter (leave default)
13. **Edit advanced config?**: Type `n` and press Enter
14. **Use this config?**: Type `y` and press Enter
15. **Quit config**: Type `q` and press Enter

## Step 3: Test the Configuration

Test if rclone can access your R2 bucket:

```bash
# List buckets
rclone lsd r2:

# List files in a bucket
rclone ls r2:aznude-clean-logs

# Test upload
echo "test" > test.txt
rclone copy test.txt r2:aznude-clean-logs/test.txt
rclone ls r2:aznude-clean-logs
rm test.txt
```

## Step 4: Update .env (if needed)

If you named your remote something other than `r2`, update your `.env`:

```bash
RCLONE_REMOTE=r2:  # Change if you used a different name
```

## Getting R2 Credentials

1. Go to Cloudflare Dashboard
2. Navigate to R2
3. Click "Manage R2 API Tokens"
4. Create API Token
5. Copy:
   - **Access Key ID**
   - **Secret Access Key**
   - **Endpoint URL** (format: `https://xxxxx.r2.cloudflarestorage.com`)
   - **Account ID** (found in R2 dashboard URL or settings)

## Troubleshooting

If you get "didn't find section in config file":
- Make sure you created the remote with the exact name you're using
- Check config file: `cat ~/.config/rclone/rclone.conf`
- The remote name should match what's in `RCLONE_REMOTE` env var (default: `r2`)

