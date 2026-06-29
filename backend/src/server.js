require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

// Import semua konfigurasi infrastruktur
const { initDB } = require('./config/db');
const { initMongo } = require('./config/mongo'); // Tambahan wajib untuk BioRoots
const { initKafka } = require('./config/kafka');

const { startConsumers } = require('./consumers/workers');
const apiRoutes = require('./routes/api');

const app = express();
const server = http.createServer(app);

// Inisialisasi Socket.io untuk Notifikasi Real-time (Topik 4)
const io = new Server(server, {
  cors: {
    origin: '*', // Saat integrasi final, batasi ini ke port Frontend (4001/4002)
    methods: ['GET', 'POST']
  }
});

app.use(cors());
app.use(express.json());

// Entry point untuk semua routing API (Kasir, Gudang, Kurir, Manajer)
app.use('/api', apiRoutes);

// Deteksi koneksi WebSocket dari Frontend EJS
io.on('connection', (socket) => {
  console.log('⚡ Client UI terkoneksi dengan Socket ID:', socket.id);
  
  socket.on('disconnect', () => {
    console.log('❌ Client UI terputus:', socket.id);
  });
});

const PORT = process.env.PORT || 3000;

async function bootstrap() {
  console.log('🌱 Memulai sistem BioRoots B2B Distributed Backend...');
  
  try {
    // 1. Inisialisasi MySQL (Master & Replica)
    await initDB();
    
    // 2. Inisialisasi MongoDB (Audit Log)
    await initMongo();
    
    // 3. Inisialisasi Kafka Cluster & Worker
    await initKafka();
    
    // Melempar instance 'io' ke worker agar consumer bisa men-trigger notifikasi ke Frontend
    await startConsumers(io);   
    
    // 4. Buka gerbang API jika semua infrastruktur di atas sukses
    server.listen(PORT, () => {
      console.log(`🚀 Backend Server BioRoots berjalan di http://localhost:${PORT}`);
    });
  } catch (error) {
    console.error('💥 Gagal melakukan bootstrap sistem. Server dihentikan.', error);
    process.exit(1); // Matikan aplikasi jika ada infrastruktur yang down
  }
}

bootstrap();