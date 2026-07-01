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
    stok_aktual_dus INT NOT NULL CHECK (stok_aktual_dus >= 0),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
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
    total_bayar DECIMAL(12,2) NOT NULL,
    status_aktual ENUM('PENDING', 'PAID', 'PACKED', 'DELIVERED') DEFAULT 'PENDING',
    id_kasir VARCHAR(20),
    id_gudang VARCHAR(20),
    id_kurir VARCHAR(20),
    waktu_transaksi TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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

-- Seed Pembeli
INSERT INTO pembeli (id_pembeli, nama, alamat, kontak) VALUES
('CUST-a1b2c3d4-e5f6-7890-abcd-123456789012', 'Budi Santoso', 'Jl. Kemerdekaan No. 45', '081234567890'),
('CUST-f6e5d4c3-b2a1-0987-dcba-210987654321', 'Koperasi Tani Makmur', 'Jl. Raya Pertanian Km 5', '081987654321');

-- Seed Inventaris (SKU menggunakan UUID)
INSERT INTO inventaris (sku, nama_bibit, harga_per_dus, stok_aktual_dus) VALUES
('SKU-11111111-2222-3333-4444-555555555555', 'Tomat Ceri (50 pcs)', 150000.00, 100),
('SKU-66666666-7777-8888-9999-000000000000', 'Cabai Rawit Merah (50 pcs)', 125000.00, 250),
('SKU-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee', 'Bawang Merah Super (50 pcs)', 200000.00, 50);