import * as crypto from 'crypto';
import { sqlQuery, escapeHardcodedValues } from './database.js';
import { config } from '../config.js';
import { bashCommand } from './fileUtils.js';
import type { RowDataPacket } from 'mysql2/promise';
import { load } from 'cheerio';

// Static pages that should be ignored
const STATIC_PAGES: string[] = [
  '/tags/tags.html',
  '/2257.html',
  '/submit.html',
  '/nonconsentual.html',
  '/privacy.html',
  '/favorites.html',
  '/dmca.html',
  '/aznudelive.html',
  '/profile.html',
  '/editprofile.html',
  '/signin.html',
  '/signup.html',
  '/upload.html',
  '/report.html',
  '/about.html',
  '/contact.html',
  '/terms.html',
  '/faq.html',
  '/support.html',
  '/communityguidelines.html',
  '/request.html',
  '/jobs.html',
  '/index.html',
  '/advertising.html',
  '/feedback.html',
  '/systemstatus.html',
  '/top100videos2020.html',
  '/top100celebs.html',
  '/top100movies.html',
  '/top100stories.html',
  '/forgetpassword.html',
  '/webmasters.html',
  '/manageuser.html',
  '/manage-content.html',
  '/search.html'
];

interface HtmlMapAssociation {
  db?: string;
  dbandtable?: string;
  id: string;
}

interface SiteConfig {
  databaseItem: string;
  site: string;
  dir: string;
  gender: string;
  gsutil: string;
}

function checkIfCelebButOldLink(uri: string): boolean {
  const pattern = /^\/[a-z]\/.*\.html$/;
  return pattern.test(uri);
}

export function whatIsGender(genderInput: string): 'f' | 'm' | 'fans' | null {
  const normalized = String(genderInput).toLowerCase().trim();
  if (normalized === 'f' || normalized === 'female' || normalized === 'women') {
    return 'f';
  } else if (normalized === 'm' || normalized === 'male' || normalized === 'men') {
    return 'm';
  } else if (normalized === 'fans') {
    return 'fans';
  }
  return null;
}

let md5Associations: Record<string, HtmlMapAssociation> = {};

export async function loadHtmlMap(): Promise<Record<string, HtmlMapAssociation>> {
  const query = "SELECT id, dbandtable, identifier FROM `html_map`;";
  const results = await sqlQuery(query, config.databases.BRAZZERS, 'select') as RowDataPacket[];
  
  md5Associations = {};
  for (const row of results) {
    const id = String(row.id);
    const dbAndTable = String(row.dbandtable);
    const identifier = String(row.identifier);
    // Match Python structure: {'db': db_and_table, 'id': identifier}
    md5Associations[id] = { db: dbAndTable, id: identifier };
  }
  
  return md5Associations;
}

async function addToHtmlMap(
  md5Item: string,
  fullUrl: string,
  site: string,
  databaseItem: string,
  table: string,
  elementId: string | number,
  activeStatus: string,
  dateToday: string
): Promise<string> {
  if (!(md5Item in md5Associations)) {
    // Match Python structure when adding: {'dbandtable': ..., 'identifier': ...}
    // But when returned, it should match loaded structure: {'db': ..., 'id': ...}
    const dbandtable = `${databaseItem}.${table}`;
    md5Associations[md5Item] = {
      db: dbandtable,
      id: String(elementId)
    };
  }

  const escapedUrl = escapeHardcodedValues(fullUrl);
  const insertQuery = `INSERT INTO \`html_map\` (\`id\`, \`url\`, \`site\`, \`dbandtable\`, \`identifier\`, \`status\`, \`date_added\`) VALUES ('${md5Item}', '${escapedUrl}', '${site}', '${databaseItem}.${table}', '${elementId}', '${activeStatus}', '${dateToday}')`;
  
  await sqlQuery(insertQuery, config.databases.BRAZZERS, 'update');
  return 'success';
}

// This function extracts the element ID from HTML files
// It fetches HTML via HTTP/HTTPS using the actual URL instead of SSH
async function getAZNudePageID(filePath: string, gender: string, stringType: string, source: string, host?: string, uri?: string): Promise<string> {
  try {
    let htmlContent: string;
    
    // If we have host and uri, fetch via HTTP/HTTPS (better than SSH)
    if (host && uri) {
      try {
        const url = `https://${host}${uri}`;
        const response = await fetch(url, {
          method: 'GET',
          headers: {
            'User-Agent': 'Mozilla/5.0 (compatible; LogProcessor/1.0)',
          },
          // Timeout after 10 seconds
          signal: AbortSignal.timeout(10000)
        });
        
        if (!response.ok) {
          // HTTP error (404, etc.) - silently return unidentified
          return 'unidentified';
        }
        
        htmlContent = await response.text();
      } catch (fetchError: any) {
        // Fetch failed - try local file or return unidentified
        console.warn(`Failed to fetch ${host}${uri}:`, fetchError.message);
        
        // Fallback: try reading as local file if path exists
        if (filePath && !filePath.startsWith('/var/www')) {
          try {
            const { readFileSync, existsSync } = await import('fs');
            if (existsSync(filePath)) {
              htmlContent = readFileSync(filePath, 'utf-8');
            } else {
              return 'unidentified';
            }
          } catch {
            return 'unidentified';
          }
        } else {
          return 'unidentified';
        }
      }
    } else {
      // No host/uri provided - try local file or SSH as last resort
      if (filePath.startsWith('/var/www') || (filePath.startsWith('/') && !filePath.startsWith('./'))) {
        // Remote server path - try SSH only if configured
        if (config.LEASEWEB_SERVER_SSH && config.LEASEWEB_SERVER_SSH !== 'ssh user@server' && config.LEASEWEB_SERVER_SSH.includes('@')) {
          try {
            const command = `${config.LEASEWEB_SERVER_SSH} cat ${filePath}`;
            htmlContent = bashCommand(command);
          } catch (sshError) {
            console.warn(`SSH command failed for ${filePath}`);
            return 'unidentified';
          }
        } else {
          return 'unidentified';
        }
      } else {
        // Local path - read directly
        const { readFileSync } = await import('fs');
        htmlContent = readFileSync(filePath, 'utf-8');
      }
    }
    
    if (!htmlContent || htmlContent.trim().length === 0) {
      return 'invalid';
    }
    
    // Parse HTML using cheerio for reliable extraction
    const $ = load(htmlContent);
    let id: string | null = null;
    
    // Method 1: Try meta tag with name="celebid" or similar
    const metaId = $(`meta[name="${stringType}id"]`).attr('content') || 
                   $(`meta[name="${stringType}_id"]`).attr('content') ||
                   $(`meta[name="${stringType}-id"]`).attr('content');
    if (metaId) {
      id = metaId.trim();
    }
    
    // Method 2: Try data attributes
    if (!id) {
      const dataId = $(`[data-${stringType}id]`).attr(`data-${stringType}id`) ||
                     $(`[data-${stringType}_id]`).attr(`data-${stringType}_id`) ||
                     $(`[data-${stringType}-id]`).attr(`data-${stringType}-id`);
      if (dataId) {
        id = dataId.trim();
      }
    }
    
    // Method 3: Try hidden input fields
    if (!id) {
      const inputId = $(`input[name="${stringType}id"]`).attr('value') ||
                      $(`input[name="${stringType}_id"]`).attr('value') ||
                      $(`input[name="${stringType}-id"]`).attr('value');
      if (inputId) {
        id = inputId.trim();
      }
    }
    
    // Method 4: Try JavaScript variables in script tags
    if (!id) {
      const scripts = $('script').toArray();
      for (const script of scripts) {
        const scriptContent = $(script).html() || '';
        const patterns = [
          new RegExp(`var\\s+${stringType}id\\s*=\\s*["']([^"']+)["']`, 'i'),
          new RegExp(`const\\s+${stringType}id\\s*=\\s*["']([^"']+)["']`, 'i'),
          new RegExp(`let\\s+${stringType}id\\s*=\\s*["']([^"']+)["']`, 'i'),
          new RegExp(`${stringType}id\\s*[:=]\\s*["']([^"']+)["']`, 'i'),
          new RegExp(`["']${stringType}id["']\\s*:\\s*["']([^"']+)["']`, 'i'),
        ];
        
        for (const pattern of patterns) {
          const match = scriptContent.match(pattern);
          if (match && match[1]) {
            id = match[1].trim();
            break;
          }
        }
        if (id) break;
      }
    }
    
    // Method 5: Try to find ID in body data attributes or common locations
    if (!id) {
      const bodyId = $('body').attr(`data-${stringType}id`) ||
                     $('body').attr(`data-${stringType}_id`) ||
                     $('body').attr(`data-${stringType}-id`);
      if (bodyId) {
        id = bodyId.trim();
      }
    }
    
    // Method 6: Fallback - search for numeric IDs in the HTML content
    if (!id) {
      // Look for patterns like "celebid": "12345" or celebid="12345"
      const fallbackPatterns = [
        new RegExp(`["']${stringType}id["']\\s*[:=]\\s*["']?(\\d+)["']?`, 'i'),
        new RegExp(`${stringType}id\\s*=\\s*["']?(\\d+)["']?`, 'i'),
        new RegExp(`id\\s*[:=]\\s*["']?(\\d{4,})["']?`, 'i'), // At least 4 digits
      ];
      
      for (const pattern of fallbackPatterns) {
        const match = htmlContent.match(pattern);
        if (match && match[1]) {
          id = match[1].trim();
          break;
        }
      }
    }
    
    if (id && id !== 'invalid' && id !== '' && !isNaN(Number(id))) {
      return id;
    }
    
    return 'invalid';
  } catch (error) {
    console.error('Error in getAZNudePageID:', error);
    console.error('File path:', filePath);
    console.error('Gender:', gender);
    console.error('Type:', stringType);
    return 'invalid';
  }
}

function getSiteConfig(host: string): SiteConfig | null {
  if (['cdn2.aznude.com', 'cdn1.aznude.com', 'www.aznude.com', 'user-uploads.aznude.com', 'aznude.com'].includes(host)) {
    return {
      databaseItem: config.databases.AZNUDE,
      site: 'aznude',
      dir: config.directories.MAIN_DIR_AZNUDE,
      gender: 'women',
      gsutil: config.gsutil.GC_WOMEN_HTML
    };
  } else if (['men.aznude.com', 'cdn-men.aznude.com', 'azmen.com', 'www.azmen.com'].includes(host)) {
    return {
      databaseItem: config.databases.AZNUDEMEN,
      site: 'aznudemen',
      dir: config.directories.MAIN_DIR_AZNUDEMEN,
      gender: 'men',
      gsutil: config.gsutil.GC_MEN_HTML
    };
  } else if (['cdn2.azfans.com', 'azfans.com', 'www.azfans.com'].includes(host)) {
    return {
      databaseItem: config.databases.AZFANS,
      site: 'azfans',
      dir: config.directories.MAIN_DIR_AZFANS,
      gender: 'fans',
      gsutil: config.gsutil.GC_FANS_HTML
    };
  }
  return null;
}

export async function identifyItem(host: string, uri: string): Promise<HtmlMapAssociation | 'invalid' | 'unidentified'> {
  const dateToday = new Date().toISOString().split('T')[0];
  
  host = host.trim().toLowerCase().split(':')[0];
  uri = uri.trim().toLowerCase();
  uri = uri.replace(/\/\//g, '/');

  // Validate host
  if (!host.includes('aznude.com') && !host.includes('azmen.com') && !host.includes('azfans.com')) {
    return 'invalid';
  }
  
  if (host === 'cdn.aznude.com') {
    return 'invalid';
  }

  // Determine site configuration
  const siteConfig = getSiteConfig(host);
  if (!siteConfig) {
    return 'invalid';
  }

  const { databaseItem, site, dir, gender, gsutil } = siteConfig;

  const fullUrl = host + uri;
  const md5Item = crypto.createHash('md5').update(fullUrl).digest('hex');

  // Check if already in cache
  if (md5Item in md5Associations) {
    return md5Associations[md5Item];
  }

  // Handle old celeb links
  if (checkIfCelebButOldLink(uri)) {
    uri = '/view/celeb' + uri;
  }

  // Various invalid checks
  if (uri.includes('/data/realitykings/') || uri.includes('/data/brazzers/') || 
      ['cdn4.aznude.com', 'cdn5.aznude.com', 'cdn3.aznude.com', 'diamond.aznude.com', 
       'search.aznude.com', 'takemen.aznude.com', 'flex.aznude.com'].includes(host)) {
    return 'invalid';
  }

  // Match Python logic exactly (note: Python has operator precedence issue but we match it)
  // Python: host == 'aznude.com' and (not uri.endswith('.html')) or '.ico' in uri or '.txt' in uri
  // This evaluates as: (host == 'aznude.com' and not uri.endswith('.html')) or '.ico' in uri or '.txt' in uri
  if (host === 'aznude.com' && !uri.endsWith('.html') || uri.includes('.ico') || uri.includes('.txt')) {
    return 'invalid';
  }
  if (host === 'azmen.com' && !uri.endsWith('.html') || uri.includes('.ico') || uri.includes('.txt')) {
    return 'invalid';
  }
  if (host === 'www.aznude.com' && !uri.endsWith('.html') || uri.includes('.ico') || uri.includes('.txt')) {
    return 'invalid';
  }
  if (host === 'www.azmen.com' && !uri.endsWith('.html') || uri.includes('.ico') || uri.includes('.txt')) {
    return 'invalid';
  }

  if (['men.aznude.com', 'www.azmen.com', 'azmen.com'].includes(host) && !uri.endsWith('.html')) {
    return 'invalid';
  }

  if (uri === '/404.html' || uri === '/403.html' || 
      (uri.includes('cr-hi') && uri.includes('embed')) || 
      (uri.includes('cr-hd') && uri.includes('embed')) || 
      uri.includes('data/men')) {
    return 'invalid';
  }

  if (['lab.aznude.com', 'preview-engine.aznude.com', 'watermark.aznude.com'].includes(host)) {
    return 'invalid';
  }

  if (uri === '.html' || uri === '/.html') {
    return 'invalid';
  }

  if (uri.endsWith('.png') || uri.includes('embed/scene_480p') || uri.includes('embed/scene_720p') || 
      uri.endsWith('largecelebpage-4.jpg') || uri.endsWith('.m3u8') || uri.endsWith('.ts') || 
      uri.endsWith('.vtt') || uri.includes('/sparkthumbs/') || uri.endsWith('.json') || uri.endsWith(',')) {
    return 'invalid';
  }

  if ((host === 'cdn2.aznude.com' || host === 'cdn1.aznude.com') && !uri.endsWith('.jpg')) {
    return 'invalid';
  }

  if ((uri.endsWith('.jpg') || uri.endsWith('.jpeg') || uri.endsWith('.gif')) && 
      ['user-uploads.aznude.com', 'cdn2.aznude.com', 'cdn1.aznude.com', 'men.aznude.com', 
       'cdn-men.aznude.com', 'www.azmen.com', 'azmen.com', 'cdn.aznude.com', 'lab.aznude.com'].includes(host)) {
    return 'invalid';
  }

  if (STATIC_PAGES.includes(uri) || 
      ['support.aznude.com', 'cdn-men.aznude.com', 'user-uploads.aznude.com', 'api.aznude.com'].includes(host) ||
      uri === '/' || uri.includes('/biopic/') || uri.includes('/boxpic/') || uri.includes('antibandit') ||
      uri.includes('/country/') || uri.includes('/browse/tags/vids') || uri.includes('/browse/videos') ||
      uri.includes('browse/images') || uri.includes('browse/celebs') || uri.includes('/browse/movies') ||
      uri.includes('/tags/vids/') || uri.includes('/tags/imgs/') || uri.includes('/browse/playlists') ||
      uri.includes('/browse/stories') || uri.includes('/browse/tags') || uri.includes('/browse/vivid') ||
      uri.includes('/browse/porn') || uri.includes('playlist-') || uri.includes('/playlists/') ||
      uri.includes('-playlist.html') || uri.includes('tags/alltags') || uri.includes('brazzers/videos') ||
      uri.includes('top100videos') || uri.includes('azncdn/vivid')) {
    return 'invalid';
  }

  if (uri.includes('view/vivid') || uri.includes('tags/') || uri.includes('browse/brazzers')) {
    return 'invalid';
  }

  // Handle video files
  if ((uri.endsWith('.mp4') || uri.endsWith('.webm')) || 
      (uri.startsWith('/embed/') && uri.endsWith('.html')) ||
      (uri.startsWith('/mrskin/') && uri.endsWith('.html')) ||
      (uri.startsWith('/azncdn/') && uri.endsWith('.html'))) {
    
    const table = 'videos';
    const elementIdLocation = 1;
    const statusLocation = 7;
    
    let file = uri.split('/').pop() || '';
    file = file.replace(/_hd\.webm|_hi\.webm|_lo\.webm|-hd\.webm|-hi\.webm|-lo\.webm/g, '')
               .replace(/_hd\.mp4|_hi\.mp4|_lo\.mp4|-hd\.mp4|-hi\.mp4|-lo\.mp4/g, '')
               .replace(/_hd\.html|_hi\.html|_lo\.html|-hd\.html|-hi\.html|-lo\.html/g, '');

    const escapedFile = escapeHardcodedValues(file);
    const query = `SELECT * FROM \`${table}\` WHERE \`thumb_id\` LIKE '%${escapedFile}%' LIMIT 1;`;

    try {
      const results = await sqlQuery(query, databaseItem, 'select') as RowDataPacket[];
      if (!results || results.length === 0) {
        if (uri.includes('embed')) {
          const domainFromGs = gsutil.replace('gs://', '');
          const serverUrl = fullUrl.replace(domainFromGs, dir);
          const command = `${config.LEASEWEB_SERVER_SSH} cat ${serverUrl}`;
          try {
            const itemSource = bashCommand(command);
            if (itemSource.includes('brazzers/videos')) {
              return 'invalid';
            } else {
              console.log('failure');
              return 'unidentified';
            }
          } catch (e) {
            console.log('failure');
            return 'unidentified';
          }
        } else {
          console.log('failure');
          return 'unidentified';
        }
      }

      const row = results[0];
      const elementId = row[elementIdLocation];
      const activeStatus = row[statusLocation];

      const addition = await addToHtmlMap(md5Item, fullUrl, site, databaseItem, table, elementId, activeStatus, dateToday);
      if (addition === 'success') {
        return md5Associations[md5Item];
      }
    } catch (error) {
      console.error('Error processing video:', error);
      console.error('Host:', host);
      console.error('URI:', uri);
      if (uri.includes('embed')) {
        return 'unidentified';
      } else {
        return 'unidentified';
      }
    }
  }
  // Handle HTML pages (celeb, movie, story)
  else if (uri.endsWith('.html') && 
           (uri.startsWith('/view/celeb') || uri.startsWith('/view/movie') || 
            uri.startsWith('/view/model') || uri.startsWith('/view/story'))) {
    
    let table: string;
    let stringType: string;
    let statusLocation: number;
    let column: string;
    
    if (uri.startsWith('/view/celeb') || uri.startsWith('/view/model')) {
      table = 'celebs';
      stringType = 'celeb';
      statusLocation = 9;
      column = 'celebid';
    } else if (uri.startsWith('/view/movie')) {
      table = 'movies';
      stringType = 'movie';
      column = 'movieid';
      statusLocation = 11;
    } else if (uri.startsWith('/view/story')) {
      table = 'stories';
      stringType = 'story';
      column = 'storyid';
      statusLocation = 12;
    } else {
      console.error('This is impossible, url doesnt start with view/celeb or view/movie');
      process.exit(1);
      // This will never execute but TypeScript needs it
      table = '';
      stringType = '';
      column = '';
      statusLocation = 0;
    }

    let elementId: string;
    try {
      // Pass host and uri so we can fetch via HTTP instead of SSH
      elementId = await getAZNudePageID(dir + uri, gender, stringType, 'cloudflarelog', host, uri);
    } catch (error) {
      console.error('Error getting page ID:', error);
      console.error('Host:', host);
      console.error('Path:', dir + uri);
      console.error('Gender:', gender);
      console.error('Type:', stringType);
      console.error('Full URL:', fullUrl);
      return 'unidentified';
    }

    if (elementId === 'invalid' || elementId === 'unidentified' || !elementId) {
      // If we can't extract the ID, return 'unidentified' (don't query with 'unidentified' as ID)
      return 'unidentified';
    }

    let query = `SELECT * FROM \`${table}\` WHERE \`${column}\` ='${elementId}' LIMIT 1;`;
    
    try {
      let results = await sqlQuery(query, databaseItem, 'select') as RowDataPacket[];
      if (!results || results.length === 0) {
        // Check if this is a duplicate that was merged
        try {
          const duplicateResults = await sqlQuery(
            `SELECT origin FROM \`duplicate-handling\` WHERE \`destination\` LIKE '${elementId}'`,
            databaseItem,
            'select'
          ) as RowDataPacket[];
          if (duplicateResults && duplicateResults.length > 0) {
            elementId = String(duplicateResults[0].origin);
            query = `SELECT * FROM \`${table}\` WHERE \`${column}\` ='${elementId}' LIMIT 1;`;
            results = await sqlQuery(query, databaseItem, 'select') as RowDataPacket[];
          } else {
            console.error('Full URL:', fullUrl);
            console.error('Query:', query);
            return 'unidentified';
          }
        } catch (e) {
          console.error('Error checking duplicates:', e);
          console.error('Full URL:', fullUrl);
          console.error('Query:', query);
          return 'unidentified';
        }
      }

      if (!results || results.length === 0) {
        return 'unidentified';
      }

      const row = results[0];
      let activeStatus: string | number = row[statusLocation];
      if (activeStatus === 0) {
        activeStatus = 'active';
      } else if (activeStatus === 1) {
        activeStatus = 'ianctive';
      }

      await addToHtmlMap(md5Item, fullUrl, site, databaseItem, table, elementId, String(activeStatus), dateToday);
      return md5Associations[md5Item];
    } catch (error) {
      console.error('Error querying database:', error);
      console.error('Full URL:', fullUrl);
      console.error('Query:', query);
      return 'unidentified';
    }
  } else {
    // Not identified - silently return (no logging needed)
    return 'unidentified';
  }
  
  // This should never be reached, but TypeScript needs it
  return 'unidentified';
}

