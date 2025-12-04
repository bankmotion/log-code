import * as crypto from 'crypto';
import { sqlQuery, escapeHardcodedValues } from './database.js';
import { config } from '../config.js';
import { bashCommand } from './fileUtils.js';
import type { RowDataPacket } from 'mysql2/promise';

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

// This function needs to be implemented based on your getAZNudePageID logic
// For now, it's a placeholder
async function getAZNudePageID(_filePath: string, _gender: string, _stringType: string, _source: string): Promise<string> {
  // This would need to parse HTML files to extract IDs
  // Implementation depends on your specific HTML structure
  // For now, return a placeholder
  throw new Error('getAZNudePageID not implemented - needs HTML parsing logic');
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
      // This function needs to be implemented
      elementId = await getAZNudePageID(dir + uri, gender, stringType, 'cloudflarelog');
    } catch (error) {
      console.error('Error getting page ID:', error);
      console.error('Host:', host);
      console.error('Path:', dir + uri);
      console.error('Gender:', gender);
      console.error('Type:', stringType);
      console.error('Full URL:', fullUrl);
      return 'unidentified';
    }

    if (elementId === 'invalid') {
      return 'invalid';
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
    console.log('not identified: ' + host + uri);
    console.log(host);
    return 'unidentified';
  }
  
  // This should never be reached, but TypeScript needs it
  return 'unidentified';
}

