const { kafka } = require('../config/kafka');
const { poolReplica } = require('../config/db');
const { initMongo } = require('../config/mongo');

const WORKER_NAME = process.env.WORKER_NAME || 'BioRoots-Worker-1';
const ROLE = process.env.ROLE || 'ALL';

let mongoDb = null;

// ==========================================
// HELPER: Pencatatan Jejak Audit ke MongoDB (Data Lake)
// ==========================================
async function logToMongoDB(topic, eventType, data) {
  if (!mongoDb) return;
  try {
    const logDoc = {
      order_id: data.order_id || null,
      sku: data.sku || null,
      waktu_kejadian: new Date(),
      aktor_sistem: WORKER_NAME,
      topik_kafka: topic,
      event_type: eventType,
      payload_snapshot: data
    };
    
    await mongoDb.collection('event_logs').insertOne(logDoc);
    console.log(`[MongoDB Audit] Logged Event: ${eventType} via ${topic}`);
  } catch (err) {
    console.error(`[MongoDB Error] Gagal mencatat log: ${err.message}`);
  }
}

// ==========================================
// HELPER: Trigger Update Dashboard Manajer
// ==========================================
async function emitUpdatedAnalytics(io) {
  try {
    if (!mongoDb) return;
    const logsCollection = mongoDb.collection('event_logs');
    
    // Agregasi hitungan event untuk chart
    const eventStats = await logsCollection.aggregate([
      { $group: { _id: "$event_type", count: { $sum: 1 } } }
    ]).toArray();

    // Broadcast ke frontend Manajer (Port 4002)
    io.emit('manager_analytics_update', { eventStats, processedBy: WORKER_NAME });
  } catch (error) { 
    console.error('[Socket Error] Gagal emit analytics:', error); 
  }
}

// ==========================================
// INISIALISASI SELURUH WORKER / CONSUMER
// ==========================================
async function startConsumers(io) {
  mongoDb = await initMongo();

  // 1. WORKER GUDANG (Mendengarkan Pesanan Lunas dari Kasir)
  if (ROLE === 'ALL' || ROLE === 'GUDANG') {
    const gudangConsumer = kafka.consumer({ groupId: 'gudang-group' });
    await gudangConsumer.connect();
    await gudangConsumer.subscribe({ topic: 'transaksi_pos', fromBeginning: false });
    
    gudangConsumer.run({
      eachMessage: async ({ topic, message }) => {
        const data = JSON.parse(message.value.toString());
        await logToMongoDB(topic, 'ORDER_PAID_AND_RECEIVED', data);
        
        // Notifikasi ke layar Gudang: "Ada pesanan baru yang harus dipacking!"
        io.emit('new_packing_task', { ...data, processedBy: WORKER_NAME });
      },
    });
  }

  // 2. WORKER KURIR (Mendengarkan Pesanan Selesai Packing)
  if (ROLE === 'ALL' || ROLE === 'KURIR') {
    const kurirConsumer = kafka.consumer({ groupId: 'kurir-group' });
    await kurirConsumer.connect();
    await kurirConsumer.subscribe({ topic: 'fulfillment_gudang', fromBeginning: false });
    
    kurirConsumer.run({
      eachMessage: async ({ topic, message }) => {
        const data = JSON.parse(message.value.toString());
        await logToMongoDB(topic, 'ORDER_PACKED_READY_TO_SHIP', data);
        
        // Notifikasi ke layar Kurir: "Ada barang siap diantar!"
        io.emit('new_delivery_task', { ...data, processedBy: WORKER_NAME });
      },
    });
  }

  // 3. WORKER KASIR & NOTIFIKASI (Mendengarkan Kurir Selesai Antar)
  if (ROLE === 'ALL' || ROLE === 'KASIR') {
    const kasirConsumer = kafka.consumer({ groupId: 'kasir-group' });
    await kasirConsumer.connect();
    await kasirConsumer.subscribe({ topic: 'pengiriman_kurir', fromBeginning: false });
    await kasirConsumer.subscribe({ topic: 'notifikasi_sistem', fromBeginning: false });
    
    kasirConsumer.run({
      eachMessage: async ({ topic, message }) => {
        const data = JSON.parse(message.value.toString());
        
        if (topic === 'pengiriman_kurir') {
          await logToMongoDB(topic, 'SHIPMENT_DELIVERED_POD', data);
          // Beritahu layar Kasir bahwa pesanan pelanggan X sudah sampai
          io.emit('delivery_completed', { ...data, processedBy: WORKER_NAME });
        } 
        else if (topic === 'notifikasi_sistem') {
          await logToMongoDB(topic, 'SYSTEM_ALERT', data);
          io.emit('system_alert', data);
        }
      },
    });
  }

  // 4. WORKER MANAJER & ANALITIK (Mendengarkan SEMUA Pergerakan)
  if (ROLE === 'ALL' || ROLE === 'MANAGER') {
    const managerConsumer = kafka.consumer({ groupId: 'manager-analytics-group' });
    await managerConsumer.connect();
    
    // Subscribe ke semua aktivitas operasional
    const topics = ['transaksi_pos', 'fulfillment_gudang', 'pengiriman_kurir', 'inbound_stok'];
    for (const t of topics) {
      await managerConsumer.subscribe({ topic: t, fromBeginning: false });
    }
    
    managerConsumer.run({ 
      eachMessage: async ({ topic, message }) => {
        const data = JSON.parse(message.value.toString());
        
        // Catat khusus untuk Inbound Stok
        if (topic === 'inbound_stok') {
            await logToMongoDB(topic, 'STOCK_REPLENISHED', data);
        }

        // Setiap ada pergerakan event apapun, paksa dashboard Manajer untuk update grafiknya
        await emitUpdatedAnalytics(io);
      }
    });
  }

  console.log(`[Worker - ${WORKER_NAME}] Kafka Consumers Active for Role: ${ROLE}`);
}

module.exports = { startConsumers };