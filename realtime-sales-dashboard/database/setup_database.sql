-- =========================================================
-- setup_database.sql
-- Jalankan file ini di MySQL Workbench atau lewat command line:
--   mysql -u root -p < setup_database.sql
-- =========================================================

-- 1. Buat database
CREATE DATABASE IF NOT EXISTS sales_dashboard
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE sales_dashboard;

-- 2. Tabel transaksi mentah (hasil tulisan dari Spark, data per transaksi)
CREATE TABLE IF NOT EXISTS transactions (
    id                INT AUTO_INCREMENT PRIMARY KEY,
    transaction_id    VARCHAR(50)   NOT NULL,
    transaction_date  DATETIME      NOT NULL,
    product_name      VARCHAR(100)  NOT NULL,
    category          VARCHAR(50)   NOT NULL,
    quantity          INT           NOT NULL,
    price             DECIMAL(12,2) NOT NULL,
    total_amount      DECIMAL(12,2) NOT NULL,
    payment_method    VARCHAR(30)   NOT NULL,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_transaction_id (transaction_id)
);

-- 3. Tabel ringkasan per window waktu (hasil agregasi Spark, untuk Power BI)
CREATE TABLE IF NOT EXISTS sales_summary (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    window_start        DATETIME NOT NULL,
    window_end          DATETIME NOT NULL,
    total_transactions  INT NOT NULL,
    total_quantity      INT NOT NULL,
    total_revenue       DECIMAL(15,2) NOT NULL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Tabel ringkasan penjualan per kategori (untuk chart "Sales per Category")
CREATE TABLE IF NOT EXISTS category_summary (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    window_start        DATETIME NOT NULL,
    window_end          DATETIME NOT NULL,
    category            VARCHAR(50) NOT NULL,
    total_quantity      INT NOT NULL,
    total_revenue       DECIMAL(15,2) NOT NULL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Tabel ringkasan per metode pembayaran (untuk chart "Payment Method Analysis")
CREATE TABLE IF NOT EXISTS payment_summary (
    id                  INT AUTO_INCREMENT PRIMARY KEY,
    window_start        DATETIME NOT NULL,
    window_end          DATETIME NOT NULL,
    payment_method       VARCHAR(30) NOT NULL,
    total_transactions  INT NOT NULL,
    total_revenue       DECIMAL(15,2) NOT NULL,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 6. Cek isi tabel (jalankan setelah sistem berjalan)
-- SELECT * FROM transactions ORDER BY transaction_date DESC LIMIT 10;
-- SELECT * FROM sales_summary ORDER BY window_start DESC LIMIT 10;
-- SELECT * FROM category_summary ORDER BY window_start DESC;
-- SELECT * FROM payment_summary ORDER BY window_start DESC;
