import { execSync } from 'child_process';
import { existsSync, readFileSync, writeFileSync, mkdirSync, rmSync, unlinkSync } from 'fs';
import { createReadStream } from 'fs';
import { createGunzip } from 'zlib';
import { createInterface } from 'readline';

export function bashCommand(command: string, timeout: number = 30000): string {
  try {
    const output = execSync(command, { 
      encoding: 'utf-8', 
      stdio: 'pipe',
      timeout: timeout,
      maxBuffer: 10 * 1024 * 1024 // 10MB buffer
    });
    return output.trim();
  } catch (error: any) {
    // Re-throw the error so callers can handle it
    throw error;
  }
}

export function shellQuote(str: string): string {
  // Simple shell quoting - escape single quotes
  return `'${str.replace(/'/g, "'\"'\"'")}'`;
}

export function removeDuplicatesFile(filePath: string): void {
  if (!existsSync(filePath)) {
    return;
  }

  const lines = new Set<string>();
  const content = readFileSync(filePath, 'utf-8');
  const fileLines = content.split('\n').filter(line => line.trim());

  for (const line of fileLines) {
    lines.add(line);
  }

  writeFileSync(filePath, Array.from(lines).join('\n') + '\n');
}

export function extractDateFromFilename(filename: string): string | null {
  // Extract date from filename like "20240625T093828Z_20240625T093849Z_0d62599e.log.gz"
  // Returns YYYYMMDD format
  const match = filename.match(/(\d{8})/);
  if (match) {
    return match[1];
  }
  return null;
}

export async function* readGzippedFile(filePath: string): AsyncGenerator<string> {
  const fileStream = createReadStream(filePath);
  const gunzip = createGunzip();
  const rl = createInterface({
    input: fileStream.pipe(gunzip),
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    if (line.trim()) {
      yield line.trim();
    }
  }
}

export async function* readTextFile(filePath: string): AsyncGenerator<string> {
  const fileStream = createReadStream(filePath);
  const rl = createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });

  for await (const line of rl) {
    if (line.trim()) {
      yield line.trim();
    }
  }
}

export function ensureDirectoryExists(dirPath: string): void {
  if (!existsSync(dirPath)) {
    mkdirSync(dirPath, { recursive: true });
  }
}

export function removeDirectory(dirPath: string): void {
  if (existsSync(dirPath)) {
    rmSync(dirPath, { recursive: true, force: true });
  }
}

export function removeFile(filePath: string): void {
  if (existsSync(filePath)) {
    unlinkSync(filePath);
  }
}

