import mysql, { Pool, PoolConnection, ResultSetHeader, RowDataPacket } from 'mysql2/promise';
import { config } from '../config.js';

let connectionPool: Pool | null = null;

function getConnection(): Pool {
  if (!connectionPool) {
    connectionPool = mysql.createPool({
      host: config.mysql.host,
      user: config.mysql.user,
      password: config.mysql.password,
      port: config.mysql.port,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0
    });
  }
  return connectionPool;
}

export async function sqlQuery(
  query: string,
  database: string,
  operation: 'select' | 'update' | 'insert' = 'select'
): Promise<RowDataPacket[] | ResultSetHeader> {
  const pool = getConnection();
  
  try {
    // Use the specific database
    await pool.query(`USE \`${database}\``);
    
    if (operation === 'select') {
      const [rows] = await pool.query<RowDataPacket[]>(query);
      return rows;
    } else if (operation === 'update' || operation === 'insert') {
      const [result] = await pool.query<ResultSetHeader>(query);
      return result;
    } else {
      throw new Error(`Unknown operation: ${operation}`);
    }
  } catch (error) {
    console.error('Database query error:', error);
    console.error('Query:', query);
    throw error;
  }
}

export function escapeHardcodedValues(value: string | number): string {
  const str = typeof value === 'string' ? value : String(value);
  // Escape single quotes and backslashes for SQL
  return str.replace(/\\/g, '\\\\').replace(/'/g, "\\'");
}

