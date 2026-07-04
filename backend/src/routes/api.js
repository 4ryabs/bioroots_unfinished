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
        const [rows] = await pool.query(`
            SELECT 
                i.sku, 
                i.nama_bibit, 
                i.harga_per_dus, 
                IFNULL(SUM(s.stok_aktual_dus), 0) AS stok_aktual_dus
            FROM inventaris i
            LEFT JOIN stok_gudang s ON i.sku = s.sku
            GROUP BY i.sku, i.nama_bibit, i.harga_per_dus
        `);
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
                pb.alamat AS alamat_pembeli,
                pb.kontak AS kontak_pembeli,
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
// 1. PELANGGAN: Buat Draft Pesanan
// ==========================================
router.post('/pelanggan/checkout', async (req, res) => {
    console.log('====== REQUEST BODY MASUK ======');
    console.log(JSON.stringify(req.body, null, 2));

    const { pembeli, items } = req.body;

    if (!pembeli || !items || items.length === 0) {
        return res.status(400).json({ error: 'Payload tidak lengkap atau keranjang kosong!' });
    }

    let finalIdPembeli = pembeli.id_pembeli;
    const newOrderId = `ORD-${uuidv4()}`;
    const total_bayar = items.reduce((sum, item) => sum + (item.subtotal || 0), 0);

    const conn = await pool.getConnection();
    try {
        await conn.beginTransaction();

        if (!finalIdPembeli) {
            finalIdPembeli = `CUST-${uuidv4()}`;
            console.log(`Menginput pembeli baru: ${pembeli.nama} (${finalIdPembeli})`);
            await conn.query(
                'INSERT INTO pembeli (id_pembeli, nama, alamat, kontak) VALUES (?, ?, ?, ?)',
                [finalIdPembeli, pembeli.nama, pembeli.alamat, pembeli.kontak]
            );
        }

        // 👇 FIX: Tambahkan id_gudang_tujuan dan set value-nya null agar MySQL tidak error
        console.log(`Menginput header pesanan: ${newOrderId}`);
        await conn.query(
            'INSERT INTO pesanan (order_id, id_pembeli, total_bayar, status_aktual, id_kasir, id_gudang_tujuan) VALUES (?, ?, ?, ?, ?, ?)',
            [newOrderId, finalIdPembeli, total_bayar, 'PENDING', 'SYSTEM_WEB','-']
        );

        for (let item of items) {
            const newDetailId = `DET-${uuidv4()}`;
            console.log(`Menginput detail pesanan: ${newDetailId} untuk SKU ${item.sku}`);
            await conn.query(
                'INSERT INTO detail_pesanan (id_detail, order_id, sku, qty_dus, subtotal) VALUES (?, ?, ?, ?, ?)',
                [newDetailId, newOrderId, item.sku, item.qty_dus, item.subtotal]
            );
        }

        await conn.commit();
        console.log(`✅ BERHASIL COMMIT: Order ${newOrderId} disimpan ke database.`);

        try {
            await sendMessage('notifikasi_sistem', {
                type: 'NEW_DRAFT',
                order_id: newOrderId,
                pesan: `Ada pesanan masuk dari ${pembeli.nama}!`
            });
        } catch (kafkaError) {
            console.error('⚠️ Kafka gagal mengirim pesan, tapi SQL tetap aman:', kafkaError.message);
        }

        res.status(201).json({ message: 'Order berhasil dibuat', order_id: newOrderId });
    } catch (error) {
        await conn.rollback();
        console.error('💥 TRANSACTION ROLLBACK. Alasan gagal:');
        console.error(error);
        res.status(500).json({ error: error.message });
    } finally {
        conn.release();
    }
});

// ==========================================
// 2. KASIR: Proses Pembayaran & Potong Stok (ACID Strict + Optimistic Locking)
// ==========================================
router.post('/kasir/pay', async (req, res) => {
    const { order_id, id_kasir, id_gudang_tujuan } = req.body;

    if (!id_gudang_tujuan) {
        return res.status(400).json({ error: 'Gudang tujuan harus dipilih!' });
    }

    const conn = await pool.getConnection();

    try {
        await conn.beginTransaction();

        // 1. Update status pesanan JIKA MASIH PENDING (Kunci Optimistic Locking)
        const [updateResult] = await conn.query(
            'UPDATE pesanan SET status_aktual = ?, id_kasir = ?, id_gudang_tujuan = ? WHERE order_id = ? AND status_aktual = "PENDING"',
            ['PAID', id_kasir, id_gudang_tujuan, order_id]
        );

        // ==========================================
        // CEGAH BENTROK / RACE CONDITION
        // Jika affectedRows === 0, berarti dalam sepersekian detik sebelumnya 
        // sudah ada kasir lain yang mengubah statusnya dari PENDING ke PAID.
        // ==========================================
        if (updateResult.affectedRows === 0) {
            await conn.rollback();
            return res.status(409).json({
                error: 'GAGAL: Pesanan ini baru saja selesai diklaim oleh kasir lain.'
            });
        }

        // 2. Ambil detail pesanan untuk memotong stok (Hanya jalan jika berhasil diklaim)
        const [items] = await conn.query('SELECT sku, qty_dus FROM detail_pesanan WHERE order_id = ?', [order_id]);

        for (let item of items) {
            const [stokResult] = await conn.query(
                'UPDATE stok_gudang SET stok_aktual_dus = stok_aktual_dus - ? WHERE sku = ? AND id_gudang = ? AND stok_aktual_dus >= ?',
                [item.qty_dus, item.sku, id_gudang_tujuan, item.qty_dus]
            );

            if (stokResult.affectedRows === 0) {
                await conn.rollback();
                return res.status(400).json({
                    error: `GAGAL: Stok untuk bibit ${item.sku} di ${id_gudang_tujuan} tidak mencukupi!`
                });
            }
        }

        await conn.commit();

        // 3. Publish Event ke Kafka agar Gudang mulai bekerja
        const eventPayload = { order_id, status: 'PAID', id_kasir, id_gudang_tujuan, items };
        await sendMessage('transaksi_pos', eventPayload);

        res.json({ message: `Lunas. Order diteruskan ke ${id_gudang_tujuan}.`, order_id });
    } catch (error) {
        await conn.rollback();
        res.status(500).json({ error: 'Terjadi kesalahan sistem: ' + error.message });
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
    const { sku, qty_masuk_dus, id_aktor_gudang, id_gudang } = req.body;
    const newInboundId = `INB-${uuidv4()}`;

    const conn = await pool.getConnection();

    try {
        await conn.beginTransaction();

        await conn.query(`
            INSERT INTO stok_gudang (sku, id_gudang, stok_aktual_dus) 
            VALUES (?, ?, ?) 
            ON DUPLICATE KEY UPDATE stok_aktual_dus = stok_aktual_dus + ?
        `, [sku, id_gudang, qty_masuk_dus, qty_masuk_dus]);

        await conn.query(
            'INSERT INTO riwayat_inbound (id_inbound, sku, qty_masuk_dus, id_aktor_gudang) VALUES (?, ?, ?, ?)',
            [newInboundId, sku, qty_masuk_dus, id_aktor_gudang]
        );

        await conn.commit();

        await sendMessage('inbound_stok', { sku, id_gudang, qty_masuk_dus, id_aktor_gudang });

        res.json({ message: `Berhasil restock ${sku} sebanyak ${qty_masuk_dus} dus di ${id_gudang}.` });
    } catch (error) {
        await conn.rollback();
        res.status(500).json({ error: error.message });
    } finally {
        conn.release();
    }
});

router.post('/manager_gudang/assign_kurir', async (req, res) => {
    const { order_id, id_kurir } = req.body;

    try {
        const [updateResult] = await pool.query(
            'UPDATE pesanan SET id_kurir = ? WHERE order_id = ? AND status_aktual = "PACKED"',
            [id_kurir, order_id]
        );

        if (updateResult.affectedRows === 0) {
            return res.status(400).json({ error: 'Gagal Assign: Pastikan pesanan sudah selesai dipacking!' });
        }

        // Boleh broadcast Kafka khusus agar aplikasi kurirnya nge-ting-tong
        await sendMessage('notifikasi_sistem', {
            type: 'KURIR_ASSIGNED',
            order_id: order_id,
            id_kurir: id_kurir,
            pesan: `Tugas pengantaran baru di-assign ke ${id_kurir}`
        });

        res.json({ message: `Berhasil menugaskan pengiriman ke ${id_kurir}`, order_id });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// ==========================================
// 5. KURIR: Pengiriman Selesai & PoD (Pool & Claim + Anti Rebutan)
// ==========================================
router.post('/kurir/deliver', async (req, res) => {
    // UPDATE: Cek juga apakah paket ini ditugaskan ke kurir yang login
    const { order_id, id_kurir, nama_penerima, catatan_pod } = req.body;

    try {
        // Hanya kurir yang di-assign oleh manager yang bisa mengantarkan!
        const [updateResult] = await pool.query(
            'UPDATE pesanan SET status_aktual = ?, nama_penerima = ?, catatan_pod = ? WHERE order_id = ? AND status_aktual = "PACKED" AND id_kurir = ?',
            ['DELIVERED', nama_penerima, catatan_pod, order_id, id_kurir]
        );

        if (updateResult.affectedRows === 0) {
            return res.status(409).json({
                error: 'GAGAL: Paket ini bukan tugas Anda, atau statusnya belum/sudah selesai.'
            });
        }

        const eventPayload = { order_id, status: 'DELIVERED', id_kurir, nama_penerima, catatan_pod };
        await sendMessage('pengiriman_kurir', eventPayload);

        res.json({ message: 'Pengiriman sukses. PoD tercatat di Database.', order_id });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// Di routes/api.js, update route /manager/analytics
router.get('/manager/analytics', async (req, res) => {
    try {
        const db = mongoClient.db('bioroots_logs');
        const logsCollection = db.collection('event_logs');

        // 1. Aggregation untuk Summary (Chart data lama)
        const eventStats = await logsCollection.aggregate([
            { $group: { _id: "$event_type", count: { $sum: 1 } } }
        ]).toArray();

        // 2. RAW LOGS: Ambil 20 event terbaru dengan waktu presisi
        const recentEvents = await logsCollection.find({})
            .sort({ waktu_kejadian: -1 })
            .limit(20)
            .toArray();

        res.json({
            message: 'Data analitik berhasil ditarik',
            eventStats,
            recentEvents
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

        const user = await karyawanCollection.findOne({ username, password });

        if (user) {
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
            { username: 'kasir1', password: '123456', role: 'kasir', nama: 'Siti Kurang' },
            { username: 'kasir2', password: '123456', role: 'kasir', nama: 'Budi Bagi' },
            { username: 'staff_gudang1', password: '123456', role: 'staff_gudang', nama: 'Bagas Wrapper' },
            { username: 'staff_gudang2', password: '123456', role: 'staff_gudang', nama: 'Bobon Galogis' },
            { username: 'staff_gudang3', password: '123456', role: 'staff_gudang', nama: 'Yanto Gudang Garam' },
            { username: 'staff_gudang4', password: '123456', role: 'staff_gudang', nama: 'Misun' },
            { username: 'kurir1', password: '123456', role: 'kurir', nama: 'Asep Racing' },
            { username: 'kurir2', password: '123456', role: 'kurir', nama: 'Sarpan Dragrace' },
            { username: 'manager', password: '123456', role: 'manager', nama: 'Bejo Makmur' },
            { username: 'manager_gudang1', password: '123456', role: 'manager_gudang', nama: 'Wowo Mono' },
            { username: 'manager_gudang2', password: '123456', role: 'manager_gudang', nama: 'Mulyono Timbun' },
        ];
        await db.collection('karyawan').insertMany(users);
        res.json({ message: 'Data karyawan berhasil ditambahkan ke MongoDB!' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

module.exports = router;