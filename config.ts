// Configuration file - Update these values based on your environment
// You can also use environment variables instead
// dotenv is loaded in index.ts before this config is imported

export interface Config {
  databases: {
    BRAZZERS: string;
    AZNUDE: string;
    AZNUDEMEN: string;
    AZFANS: string;
  };
  mysql: {
    host: string;
    user: string;
    password: string;
    port: number;
  };
  s3: {
    endpoint: string;
    profile: string;
    region: string;
  };
  directories: {
    MAIN_DIR_AZNUDE: string;
    MAIN_DIR_AZNUDEMEN: string;
    MAIN_DIR_AZFANS: string;
    CLOUDFLARE_LOG_DIR: string;
  };
  gsutil: {
    GC_WOMEN_HTML: string;
    GC_MEN_HTML: string;
    GC_FANS_HTML: string;
  };
  LEASEWEB_SERVER_SSH: string;
  rclone: {
    enabled: boolean;
    remote: string;
  };
}

export const config: Config = {
  // Database configurations
  databases: {
    BRAZZERS: process.env.DATABASE_BRAZZERS || 'brazzers',
    AZNUDE: process.env.DATABASE_AZNUDE || 'aznude',
    AZNUDEMEN: process.env.DATABASE_AZNUDEMEN || 'aznudemen',
    AZFANS: process.env.DATABASE_AZFANS || 'azfans'
  },

  // MySQL connection
  mysql: {
    host: process.env.MYSQL_HOST || 'localhost',
    user: process.env.MYSQL_USER || 'root',
    password: process.env.MYSQL_PASSWORD || '',
    port: parseInt(process.env.MYSQL_PORT || '3306', 10)
  },

  // S3/Wasabi configuration
  s3: {
    endpoint: process.env.WASABI_ENDPOINT || 'https://s3.us-east-2.wasabisys.com',
    profile: process.env.AWS_PROFILE || 'wasabi',
    region: process.env.AWS_REGION || 'us-east-2'
  },

  // Directory paths
  directories: {
    MAIN_DIR_AZNUDE: process.env.MAIN_DIR_AZNUDE || '/var/www/html/aznbaby',
    MAIN_DIR_AZNUDEMEN: process.env.MAIN_DIR_AZNUDEMEN || '/var/www/html/aznbaby/aznudemen',
    MAIN_DIR_AZFANS: process.env.MAIN_DIR_AZFANS || '/var/www/html/aznbaby/azfans',
    // Use local logs folder in project directory for development
    CLOUDFLARE_LOG_DIR: process.env.CLOUDFLARE_LOG_DIR || './logs'
  },

  // GS/Cloud storage paths
  gsutil: {
    GC_WOMEN_HTML: process.env.GC_WOMEN_HTML || 'gs://aznude-html',
    GC_MEN_HTML: process.env.GC_MEN_HTML || 'gs://azmen-html',
    GC_FANS_HTML: process.env.GC_FANS_HTML || 'gs://azfans-html'
  },

  // SSH/Server commands
  // Can specify SSH key file: ssh -i /path/to/key user@host
  LEASEWEB_SERVER_SSH: process.env.LEASEWEB_SERVER_SSH || 'ssh user@server',

  // Rclone configuration
  rclone: {
    enabled: process.env.RCLONE_ENABLED === 'true',
    remote: process.env.RCLONE_REMOTE || 'r2:'
  }
};

