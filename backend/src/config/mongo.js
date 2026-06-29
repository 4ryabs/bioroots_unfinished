const { MongoClient } = require('mongodb');

// URL default MongoDB lokal dari Docker
const url = 'mongodb://localhost:27017';
const client = new MongoClient(url);

const dbName = 'bioroots_logs'; 

async function initMongo() {
  try {
    await client.connect();
    console.log('✅ Terhubung ke MongoDB (BioRoots Audit Logs & Analytics)');
    
    return client.db(dbName); 
  } catch (error) {
    console.error('❌ Gagal terhubung ke MongoDB:', error.message);
    return null;
  }
}

module.exports = { initMongo, mongoClient: client };