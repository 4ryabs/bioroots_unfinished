const express = require('express');
const router = express.Router();
const { pool } = require('../config/db');
const { mongoClient } = require('../config/mongo');
const { sendMessage } = require('../config/kafka');
const { v4: uuidv4 } = require('uuid');

// ==========================================
// 0. ROUTE UMUM (Untuk Dropdown Form UI)
// ==========================================
router.get('/inventaris', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT * FROM inventaris');
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

router.get('/pesanan', async (req, res) => {
    try {
        const [rows] = await pool.query(`
            SELECT 
                p.*, 
                pb.nama AS nama_pembeli,
                (
                    SELECT JSON_ARRAYAGG(
                        JSON_OBJECT(
                            'sku', dp.sku, 
                            'nama_bibit', inv.nama_bibit, 
                            'qty_dus', dp.qty_dus
                        )
                    ) 
                    FROM detail_pesanan dp 
                    JOIN inventaris inv ON dp.sku = inv.sku
                    WHERE dp.order_id = p.order_id
                ) as items
            FROM pesanan p
            LEFT JOIN pembeli pb ON p.id_pembeli = pb.id_pembeli
            ORDER BY p.waktu_transaksi DESC
        `);
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

router.get('/pembeli', async (req, res) => {
    try {
        const [rows] = await pool.query('SELECT id_pembeli, nama, alamat, kontak FROM pembeli');
        res.json(rows);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==========================================
// 1. PELANGGAN: Buat Draft Pesanan (Multi-Item)
// ==========================================
router.post('/pelanggan/checkout', async (req, res) => {
    const { pembeli, items } = req.body;
    let finalIdPembeli = pembeli.id_pembeli;
    const newOrderId = `ORD-${uuidv4()}`;

    // Hitung total bayar di sisi server demi keamanan
    const total_bayar = items.reduce((sum, item) => sum + item.subtotal, 0);

    const conn = await pool.getConnection();
    try {
        await conn.beginTransaction();

        // 1. UPSERT PELANGGAN (Buat baru jika ID kosong)
        if (!finalIdPembeli) {
            finalIdPembeli = `CUST-${uuidv4()}`;
            await conn.query(
                'INSERT INTO pembeli (id_pembeli, nama, alamat, kontak) VALUES (?, ?, ?, ?)',
                [finalIdPembeli, pembeli.nama, pembeli.alamat, pembeli.kontak]
            );
        }

        // 2. INSERT HEADER PESANAN
        await conn.query(
            'INSERT INTO pesanan (order_id, id_pembeli, total_bayar, status_aktual, id_kasir) VALUES (?, ?, ?, ?, ?)',
            [newOrderId, finalIdPembeli, total_bayar, 'PENDING', 'SYSTEM_WEB']
        );

        // 3. INSERT MULTI-ITEM
        for (let item of items) {
            const newDetailId = `DET-${uuidv4()}`;
            await conn.query(
                'INSERT INTO detail_pesanan (id_detail, order_id, sku, qty_dus, subtotal) VALUES (?, ?, ?, ?, ?)',
                [newDetailId, newOrderId, item.sku, item.qty_dus, item.subtotal]
            );
        }

        await conn.commit();

        // 4. BROADCAST KAFKA
        await sendMessage('notifikasi_sistem', {
            type: 'NEW_DRAFT',
            order_id: newOrderId,
            pesan: `Ada pesanan masuk dari ${pembeli.nama}!`
        });

        res.status(201).json({ message: 'Order berhasil dibuat', order_id: newOrderId });
    } catch (error) {
        await conn.rollback();
        res.status(500).json({ error: error.message });
    } finally {
        conn.release();
    }
});

// ==========================================
// 2. KASIR: Proses Pembayaran & Potong Stok (ACID Strict)
// ==========================================
router.post('/kasir/pay', async (req, res) => {
    const { order_id, id_kasir } = req.body;
    const conn = await pool.getConnection();

    try {
        await conn.beginTransaction();

        // 1. Update status pesanan jadi PAID
        await conn.query(
            'UPDATE pesanan SET status_aktual = ?, id_kasir = ? WHERE order_id = ? AND status_aktual = "PENDING"',
            ['PAID', id_kasir, order_id]
        );

        // 2. Ambil detail pesanan untuk memotong stok
        const [items] = await conn.query('SELECT sku, qty_dus FROM detail_pesanan WHERE order_id = ?', [order_id]);

        for (let item of items) {
            // Pemotongan stok aktual
            await conn.query(
                'UPDATE inventaris SET stok_aktual_dus = stok_aktual_dus - ? WHERE sku = ? AND stok_aktual_dus >= ?',
                [item.qty_dus, item.sku, item.qty_dus]
            );
        }

        await conn.commit();

        // 3. Publish Event ke Kafka agar Gudang mulai bekerja
        const eventPayload = { order_id, status: 'PAID', id_kasir, items };
        await sendMessage('transaksi_pos', eventPayload);

        res.json({ message: 'Pembayaran Lunas. Stok terpotong. Instruksi ke gudang telah dikirim.', order_id });
    } catch (error) {
        await conn.rollback();
        res.status(500).json({ error: 'Gagal proses bayar atau stok tidak cukup: ' + error.message });
    } finally {
        conn.release();
    }
});

// ==========================================
// 3. GUDANG: Konfirmasi Selesai Packing (Outbound)
// ==========================================
router.post('/gudang/pack', async (req, res) => {
    const { order_id, id_aktor_gudang } = req.body;

    try {
        await pool.query('UPDATE pesanan SET status_aktual = ? WHERE order_id = ?', ['PACKED', order_id]);

        const eventPayload = { order_id, status: 'PACKED', id_aktor_gudang };

        // Kirim event ke topik fulfillment
        await sendMessage('fulfillment_gudang', eventPayload);

        res.json({ message: 'Pesanan selesai dipacking dan siap di-pickup Kurir', order_id });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

router.post('/gudang/inbound', async (req, res) => {
    const { sku, qty_masuk_dus, id_aktor_gudang } = req.body;
    const newInboundId = `INB-${uuidv4()}`; // GENERATE: UUID untuk Riwayat Inbound

    const conn = await pool.getConnection();

    try {
        await conn.beginTransaction();

        await conn.query('UPDATE inventaris SET stok_aktual_dus = stok_aktual_dus + ? WHERE sku = ?', [qty_masuk_dus, sku]);

        await conn.query(
            'INSERT INTO riwayat_inbound (id_inbound, sku, qty_masuk_dus, id_aktor_gudang) VALUES (?, ?, ?, ?)',
            [newInboundId, sku, qty_masuk_dus, id_aktor_gudang]
        );

        await conn.commit();

        // Publish event bahwa ada restock
        await sendMessage('inbound_stok', { sku, qty_masuk_dus, id_aktor_gudang });

        res.json({ message: `Berhasil restock ${sku} sebanyak ${qty_masuk_dus} dus.` });
    } catch (error) {
        await conn.rollback();
        res.status(500).json({ error: error.message });
    } finally {
        conn.release();
    }
});

// ==========================================
// 5. KURIR: Pengiriman Selesai & PoD
// ==========================================
router.post('/kurir/deliver', async (req, res) => {
    // BERUBAH: Sesuai dengan payload EJS Kurir yang baru (teks POD)
    const { order_id, id_kurir, nama_penerima, catatan_pod } = req.body;

    try {
        await pool.query('UPDATE pesanan SET status_aktual = ? WHERE order_id = ?', ['DELIVERED', order_id]);

        const eventPayload = { order_id, status: 'DELIVERED', id_kurir, nama_penerima, catatan_pod };

        // Trigger topik kurir, nanti notifikasi WebSocket akan menyambar event ini
        await sendMessage('pengiriman_kurir', eventPayload);

        res.json({ message: 'Pengiriman sukses. PoD tercatat.', order_id });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==========================================
// 6. MANAJER: Tarik Analitik dari MONGODB
// ==========================================
router.get('/manager/analytics', async (req, res) => {
    try {
        const db = mongoClient.db('bioroots_logs');
        const logsCollection = db.collection('event_logs');

        // Contoh Aggregation: Menghitung total event berdasarkan divisi/topik
        const eventStats = await logsCollection.aggregate([
            { $group: { _id: "$event_type", count: { $sum: 1 } } }
        ]).toArray();

        // Contoh Aggregation: Mengambil riwayat inbound restock terbaru
        const recentInbounds = await logsCollection.find({ event_type: "STOCK_REPLENISHED" })
            .sort({ waktu_kejadian: -1 })
            .limit(5)
            .toArray();

        res.json({
            message: 'Data analitik berhasil ditarik dari MongoDB',
            eventStats,
            recentInbounds
        });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

router.post('/auth/login', async (req, res) => {
    const { username, password } = req.body;

    try {
        const db = mongoClient.db('bioroots_logs');
        const karyawanCollection = db.collection('karyawan');

        // Cari user berdasarkan kredensial
        const user = await karyawanCollection.findOne({ username, password });

        if (user) {
            // Jangan kirim password kembali ke Frontend
            res.json({
                success: true,
                user: {
                    username: user.username,
                    role: user.role,
                    nama: user.nama
                }
            });
        } else {
            res.status(401).json({ success: false, message: 'Username atau password salah!' });
        }
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});


// ==========================================
// 8. AUTHENTICATION: Seed Data Karyawan (MongoDB)
// ==========================================
router.get('/auth/seed', async (req, res) => {
    try {
        const db = mongoClient.db('bioroots_logs');
        const users = [
            { username: 'kurir1', password: '123456', role: 'kurir', nama: 'Slamet Racing' },
            { username: 'manager', password: '123456', role: 'manager', nama: 'Sriyanto' },
        ];
        await db.collection('karyawan').insertMany(users);
        res.json({ message: 'Data karyawan berhasil ditambahkan ke MongoDB!' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

module.exports = router;