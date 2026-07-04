-- Hapus database jika sudah ada (Sangat berguna saat rebuild Docker)
DROP DATABASE IF EXISTS bioroots_db;
CREATE DATABASE bioroots_db;
USE bioroots_db;

-- ==========================================
-- 1. TABEL PEMBELI (B2B & B2C)
-- ==========================================
CREATE TABLE pembeli (
    id_pembeli VARCHAR(50) PRIMARY KEY,
    nama VARCHAR(100) NOT NULL,
    alamat TEXT NOT NULL,
    kontak VARCHAR(15) NOT NULL
);

-- ==========================================
-- 2. TABEL INVENTARIS (SKU)
-- ==========================================
CREATE TABLE inventaris (
    sku VARCHAR(50) PRIMARY KEY,
    nama_bibit VARCHAR(100) NOT NULL,
    harga_per_dus DECIMAL(10,2) NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE stok_gudang (
    sku VARCHAR(50) NOT NULL,
    id_gudang VARCHAR(50) NOT NULL,
    stok_aktual_dus INT NOT NULL CHECK (stok_aktual_dus >= 0),
    PRIMARY KEY (sku, id_gudang)
);

-- ==========================================
-- 3. TABEL RIWAYAT INBOUND
-- ==========================================
CREATE TABLE riwayat_inbound (
    id_inbound VARCHAR(50) PRIMARY KEY,
    sku VARCHAR(50) NOT NULL,
    qty_masuk_dus INT NOT NULL,
    id_aktor_gudang VARCHAR(20) NOT NULL,
    waktu_masuk TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sku) REFERENCES inventaris(sku) ON DELETE CASCADE
);

-- ==========================================
-- 4. TABEL PESANAN (HEADER)
-- ==========================================
CREATE TABLE pesanan (
    order_id VARCHAR(50) PRIMARY KEY,
    id_pembeli VARCHAR(50) NOT NULL,
    id_gudang_tujuan VARCHAR(50) NOT NULL,
    total_bayar DECIMAL(12,2) NOT NULL,
    status_aktual ENUM('PENDING', 'PAID', 'PACKED', 'DELIVERED') DEFAULT 'PENDING',
    id_kasir VARCHAR(20),
    id_gudang VARCHAR(20),
    id_kurir VARCHAR(20),
    waktu_transaksi TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    nama_penerima VARCHAR(255) NULL, 
    catatan_pod TEXT NULL,
    FOREIGN KEY (id_pembeli) REFERENCES pembeli(id_pembeli) ON DELETE CASCADE
);

-- ==========================================
-- 5. TABEL DETAIL PESANAN (ITEM)
-- ==========================================
CREATE TABLE detail_pesanan (
    id_detail VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    sku VARCHAR(50) NOT NULL,
    qty_dus INT NOT NULL,
    subtotal DECIMAL(10,2) NOT NULL,
    FOREIGN KEY (order_id) REFERENCES pesanan(order_id) ON DELETE CASCADE,
    FOREIGN KEY (sku) REFERENCES inventaris(sku) ON DELETE CASCADE
);

-- ==========================================
-- SEEDING DUMMY DATA (DENGAN FORMAT UUID)
-- ==========================================

-- Seed Inventaris (Master Katalog, SEKARANG TANPA KOLOM STOK)
INSERT INTO inventaris (sku, nama_bibit, harga_per_dus) VALUES
('SKU-11111111-2222-3333-4444-555555555555', 'Tomat Ceri (50 pcs)', 150000.00),
('SKU-66666666-7777-8888-9999-000000000000', 'Cabai Rawit Merah (50 pcs)', 125000.00),
('SKU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', 'Bawang Merah Super (50 pcs)', 200000.00);

-- Seed Stok Gudang (Alokasi Fisik Barang dipecah ke Gudang 1 dan Gudang 2)
INSERT INTO stok_gudang (sku, id_gudang, stok_aktual_dus) VALUES
-- Stok Tomat Ceri (Total di sistem: 150)
('SKU-11111111-2222-3333-4444-555555555555', 'gudang1', 100),
('SKU-11111111-2222-3333-4444-555555555555', 'gudang2', 50),

-- Stok Cabai Rawit Merah (Total di sistem: 225)
('SKU-66666666-7777-8888-9999-000000000000', 'gudang1', 75),
('SKU-66666666-7777-8888-9999-000000000000', 'gudang2', 150),

-- Stok Bawang Merah Super (Total di sistem: 280)
('SKU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', 'gudang1', 200),
('SKU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', 'gudang2', 80);