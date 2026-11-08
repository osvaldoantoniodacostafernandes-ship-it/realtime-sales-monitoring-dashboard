# ==============================================================================
# database/init_complete_schema.py
# Inisialisasi Skema Lengkap Big Data Capstone:
# - Streaming tables: transactions, sales_summary, category_summary, payment_summary
# - Catalog table: product_catalog (untuk Spark Stream & Batch JOIN)
# - Batch tables: daily_sales_batch, top_products_batch
# - Machine Learning tables: ml_sales_forecast, ml_anomaly_alerts, ml_product_clusters
# ==============================================================================

import os
import sys
import csv
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import mysql.connector
from config.config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

def init_schema():
    print("=" * 60)
    print("INISIALISASI SKEMA DATABASE LENGKAP: sales_dashboard")
    print("=" * 60)

    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )
    cursor = conn.cursor()

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DATABASE} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    cursor.execute(f"USE {MYSQL_DATABASE};")

    tables = {
        "transactions": """
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
        """,
        "sales_summary": """
            CREATE TABLE IF NOT EXISTS sales_summary (
                id                  INT AUTO_INCREMENT PRIMARY KEY,
                window_start        DATETIME NOT NULL,
                window_end          DATETIME NOT NULL,
                total_transactions  INT NOT NULL,
                total_quantity      INT NOT NULL,
                total_revenue       DECIMAL(15,2) NOT NULL,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        "category_summary": """
            CREATE TABLE IF NOT EXISTS category_summary (
                id                  INT AUTO_INCREMENT PRIMARY KEY,
                window_start        DATETIME NOT NULL,
                window_end          DATETIME NOT NULL,
                category            VARCHAR(50) NOT NULL,
                total_quantity      INT NOT NULL,
                total_revenue       DECIMAL(15,2) NOT NULL,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        "payment_summary": """
            CREATE TABLE IF NOT EXISTS payment_summary (
                id                  INT AUTO_INCREMENT PRIMARY KEY,
                window_start        DATETIME NOT NULL,
                window_end          DATETIME NOT NULL,
                payment_method      VARCHAR(30) NOT NULL,
                total_transactions  INT NOT NULL,
                total_revenue       DECIMAL(15,2) NOT NULL,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        "product_catalog": """
            CREATE TABLE IF NOT EXISTS product_catalog (
                product_id          VARCHAR(50) PRIMARY KEY,
                product_name        VARCHAR(100) NOT NULL UNIQUE,
                category            VARCHAR(50)  NOT NULL,
                supplier            VARCHAR(100) NOT NULL,
                margin_rate         DECIMAL(5,2) NOT NULL DEFAULT 0.25,
                unit_cost           DECIMAL(12,2) NOT NULL DEFAULT 0.00,
                base_price          DECIMAL(12,2) NOT NULL DEFAULT 0.00,
                stock_tier          VARCHAR(20)  NOT NULL DEFAULT 'Medium',
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        "daily_sales_batch": """
            CREATE TABLE IF NOT EXISTS daily_sales_batch (
                id                  INT AUTO_INCREMENT PRIMARY KEY,
                report_date         DATE NOT NULL UNIQUE,
                total_transactions  INT NOT NULL,
                total_quantity      INT NOT NULL,
                total_revenue       DECIMAL(15,2) NOT NULL,
                avg_order_value     DECIMAL(12,2) NOT NULL,
                peak_hour           INT NOT NULL,
                batch_run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        "top_products_batch": """
            CREATE TABLE IF NOT EXISTS top_products_batch (
                id                  INT AUTO_INCREMENT PRIMARY KEY,
                product_name        VARCHAR(100) NOT NULL,
                category            VARCHAR(50) NOT NULL,
                total_units_sold    INT NOT NULL,
                total_revenue       DECIMAL(15,2) NOT NULL,
                sales_rank          INT NOT NULL,
                batch_run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        "ml_sales_forecast": """
            CREATE TABLE IF NOT EXISTS ml_sales_forecast (
                id                  INT AUTO_INCREMENT PRIMARY KEY,
                forecast_time       DATETIME NOT NULL,
                predicted_revenue   DECIMAL(15,2) NOT NULL,
                lower_bound         DECIMAL(15,2) NOT NULL,
                upper_bound         DECIMAL(15,2) NOT NULL,
                model_name          VARCHAR(50) NOT NULL,
                mae_metric          DECIMAL(10,4) NOT NULL,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        "ml_anomaly_alerts": """
            CREATE TABLE IF NOT EXISTS ml_anomaly_alerts (
                id                  INT AUTO_INCREMENT PRIMARY KEY,
                transaction_id      VARCHAR(50) NOT NULL,
                transaction_date    DATETIME NOT NULL,
                product_name        VARCHAR(100) NOT NULL,
                total_amount        DECIMAL(12,2) NOT NULL,
                anomaly_score       DECIMAL(8,4) NOT NULL,
                anomaly_type        VARCHAR(50) NOT NULL,
                is_flagged          TINYINT(1) DEFAULT 1,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
        "ml_product_clusters": """
            CREATE TABLE IF NOT EXISTS ml_product_clusters (
                id                  INT AUTO_INCREMENT PRIMARY KEY,
                product_name        VARCHAR(100) NOT NULL,
                category            VARCHAR(50) NOT NULL,
                total_units         INT NOT NULL,
                total_revenue       DECIMAL(15,2) NOT NULL,
                cluster_id          INT NOT NULL,
                cluster_label       VARCHAR(50) NOT NULL,
                created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    }

    for table_name, ddl in tables.items():
        cursor.execute(ddl)
        print(f"[OK] Tabel '{table_name}' siap.")

    # Pastikan kolom feature engineering ada di tabel transactions
    ensure_transaction_feature_columns(cursor)

    # Populate product_catalog from CSV
    populate_product_catalog(cursor)

    conn.commit()
    cursor.close()
    conn.close()
    print("=" * 60)
    print("SEMUA TABEL BERHASIL DIINISIALISASI & TERISI!")
    print("=" * 60)

def ensure_transaction_feature_columns(cursor):
    cursor.execute("""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'transactions'
    """)
    existing_cols = {row[0].lower() for row in cursor.fetchall()}
    
    new_cols = [
        ("supplier", "VARCHAR(100) DEFAULT 'General Dili Trading'"),
        ("margin_rate", "DECIMAL(5,2) DEFAULT 0.25"),
        ("estimated_profit", "DECIMAL(12,2) DEFAULT 0.00"),
        ("is_high_value", "TINYINT(1) DEFAULT 0"),
        ("is_peak_hour", "TINYINT(1) DEFAULT 0"),
        ("bulk_tier", "VARCHAR(20) DEFAULT 'Single'")
    ]

    for col_name, col_def in new_cols:
        if col_name.lower() not in existing_cols:
            cursor.execute(f"ALTER TABLE transactions ADD COLUMN {col_name} {col_def}")
            print(f"[FEATURE ENG] Kolom '{col_name}' berhasil ditambahkan ke 'transactions'.")
        else:
            print(f"[FEATURE ENG] Kolom '{col_name}' sudah ada di 'transactions'.")

def populate_product_catalog(cursor):
    csv_file = PROJECT_ROOT / "producer" / "data" / "product_master_lapangan_cleaned.csv"
    if not csv_file.exists():
        print(f"[WARN] CSV {csv_file} tidak ditemukan, skip populate.")
        return

    # Kategori margin rate estimasi
    margins = {
        "Beverages": 0.35,
        "Food": 0.30,
        "Groceries": 0.20,
        "Alcohol": 0.30,
        "Electronics": 0.25,
        "Needs Verification": 0.22,
    }

    # Supplier Timor-Leste
    suppliers = {
        "Beverages": "Dili Beverage Distribution Lda",
        "Food": "Timor Food Supplier Timor-Leste",
        "Groceries": "Comoro Wholesale Groceries",
        "Alcohol": "Timor Import Wine & Spirits",
        "Electronics": "Dili Tech Store Distributor",
        "Needs Verification": "General Dili Trading",
    }

    inserted = 0
    with open(csv_file, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            p_id = row.get("product_id", "").strip()
            p_name = row.get("product_name", "").strip()
            p_cat = row.get("category", "Groceries").strip()
            price_str = row.get("dataset_price_usd", "").strip()

            if not price_str:
                continue
            try:
                price = float(price_str)
            except ValueError:
                continue

            margin = margins.get(p_cat, 0.25)
            supplier = suppliers.get(p_cat, "General Dili Trading")
            unit_cost = round(price * (1.0 - margin), 2)
            tier = "High" if price >= 30 else ("Medium" if price >= 10 else "Low")

            sql = """
                INSERT INTO product_catalog 
                    (product_id, product_name, category, supplier, margin_rate, unit_cost, base_price, stock_tier)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    category=VALUES(category),
                    margin_rate=VALUES(margin_rate),
                    unit_cost=VALUES(unit_cost),
                    base_price=VALUES(base_price),
                    stock_tier=VALUES(stock_tier)
            """
            cursor.execute(sql, (p_id, p_name, p_cat, supplier, margin, unit_cost, price, tier))
            inserted += 1

    print(f"[CATALOG] {inserted} data produk berhasil dimasukkan ke 'product_catalog'.")

if __name__ == "__main__":
    init_schema()
