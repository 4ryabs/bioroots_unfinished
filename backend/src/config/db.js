const mysql = require('mysql2/promise');

// Pool untuk MySQL Utama (Master) - Port 3306
const poolMaster = mysql.createPool({
  host: 'localhost',
  user: 'root',
  password: 'rootpassword',
  database: 'bioroots_db',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// Pool untuk MySQL Replika (Secondary) - Port 3307
const poolReplica = mysql.createPool({
  host: 'localhost',
  port: 3307,
  user: 'root',
  password: 'rootpassword',
  database: 'bioroots_db',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

async function initDB() {
  try {
    // Inisialisasi Master
    const connMaster = await poolMaster.getConnection();
    console.log('✅ Terhubung ke MySQL Master (Port 3306) - BioRoots DB');
    connMaster.release();

    // Inisialisasi Replika
    const connReplica = await poolReplica.getConnection();
    console.log('✅ Terhubung ke MySQL Replika (Port 3307) - BioRoots DB Backup');
    connReplica.release();
  } catch (error) {
    console.error('❌ Gagal terhubung ke MySQL:', error.message);
  }
}

module.exports = { pool: poolMaster, poolReplica, initDB };