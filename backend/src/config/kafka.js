const { Kafka } = require('kafkajs');

// Inisialisasi client Kafka untuk BioRoots
// Terhubung ke 3 broker sesuai konfigurasi docker-compose dari dosen
const kafka = new Kafka({
  clientId: 'bioroots-b2b-app',
  brokers: ['localhost:9092', 'localhost:9093', 'localhost:9094'],
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

// Buat instance Producer
const producer = kafka.producer();

// Fungsi inisialisasi koneksi producer dan pembuatan topik
async function initKafka() {
  try {
    const admin = kafka.admin();
    await admin.connect();
    
    // Daftar 4 topik BioRoots yang dibutuhkan dengan 5 partisi (sesuai docker-compose)
    const topicsToCreate = [
      { topic: 'transaksi_pos', numPartitions: 5 },
      { topic: 'fulfillment_gudang', numPartitions: 5 },
      { topic: 'inbound_stok', numPartitions: 5 },
      { topic: 'pengiriman_kurir', numPartitions: 5 },
      { topic: 'notifikasi_sistem', numPartitions: 5 }
    ];

    // Dapatkan topik yang sudah ada di cluster
    const existingTopics = await admin.listTopics();
    
    // Filter hanya topik yang belum dibuat
    const topicsToCreateFiltered = topicsToCreate.filter(t => !existingTopics.includes(t.topic));

    if (topicsToCreateFiltered.length > 0) {
      await admin.createTopics({
        topics: topicsToCreateFiltered,
      });
      console.log('✅ Topik Kafka BioRoots berhasil dibuat:', topicsToCreateFiltered.map(t => t.topic).join(', '));
    } else {
      console.log('⚡ Topik Kafka BioRoots sudah tersedia.');
    }

    await admin.disconnect();
    
    // Connect Producer sekali di awal saat server menyala
    await producer.connect();
    console.log('🚀 Kafka Producer berhasil terhubung dan siap mengirim event.');
  } catch (error) {
    console.error('❌ Error saat inisialisasi Kafka:', error);
  }
}

// Helper untuk mengirim pesan (Event)
async function sendMessage(topic, messageObj) {
  try {
    await producer.send({
      topic: topic,
      messages: [
        { value: JSON.stringify(messageObj) },
      ],
    });
    console.log(`[Kafka] Event berhasil dikirim ke topik: ${topic}`);
  } catch (error) {
    console.error(`[Kafka] Gagal mengirim pesan ke topik ${topic}:`, error);
  }
}

module.exports = {
  kafka,
  producer,
  initKafka,
  sendMessage
};