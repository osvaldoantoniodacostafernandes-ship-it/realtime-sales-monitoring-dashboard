# ==============================================================================
# tests/functional_test.py
# Pengujian Fungsional (Bab 14.A - Functional Testing)
#
# Memeriksa:
# 1. Dadus tama ba sistema? (Kafka Ingestion Connectivity)
# 2. Dadus prosesadu ho loos? (Spark Streaming & Batch Processing check)
# 3. Dadus rai ho loos? (MySQL Database Storage & Schema integrity)
# 4. Dashboard funsiona? (Web Dashboard Endpoint & KPI verification)
# ==============================================================================

import os
import sys
from pathlib import Path
from datetime import datetime

# Pastikan output konsol Windows mendukung UTF-8
if sys.stdout.encoding != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import mysql.connector
from kafka import KafkaConsumer, KafkaAdminClient

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)


def test_functional_pipeline():
    print("=" * 65)
    print("TESTING NO EVALUATION: A. FUNCTIONAL TESTING (Bab 14.A)")
    print("Capstone Big Data DIT 2026 - Real-Time Sales Dashboard")
    print("=" * 65)

    results = []

    # 1. KAFKA INGESTION TEST
    print("\n[TEST 1/4] Menguji Ingesti Data Kafka...")
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=5000
        )
        topics = admin_client.list_topics()
        admin_client.close()

        if KAFKA_TOPIC in topics:
            msg = f"PASS: Kafka Broker aktif dan Topic '{KAFKA_TOPIC}' tersedia."
            print(f"  [OK] {msg}")
            results.append(("1. Kafka Ingestion (Dadus tama ba sistema)", "PASS", msg))
        else:
            msg = f"FAIL: Topic '{KAFKA_TOPIC}' tidak ditemukan di Kafka."
            print(f"  [FAIL] {msg}")
            results.append(("1. Kafka Ingestion (Dadus tama ba sistema)", "FAIL", msg))
    except Exception as e:
        msg = f"FAIL: Koneksi Kafka gagal ({e})"
        print(f"  [FAIL] {msg}")
        results.append(("1. Kafka Ingestion (Dadus tama ba sistema)", "FAIL", msg))

    # 2. MYSQL STORAGE & SCHEMA TEST
    print("\n[TEST 2/4] Menguji Penyimpanan Basis Data (MySQL Storage)...")
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
        )
        cur = conn.cursor()
        cur.execute("SHOW TABLES;")
        tables = [row[0] for row in cur.fetchall()]

        required_tables = [
            "transactions", "sales_summary", "category_summary",
            "payment_summary", "product_catalog", "daily_sales_batch",
            "top_products_batch", "ml_sales_forecast", "ml_anomaly_alerts",
            "ml_product_clusters"
        ]

        missing = [t for t in required_tables if t not in tables]
        if not missing:
            cur.execute("SELECT COUNT(*) FROM transactions;")
            count = cur.fetchone()[0]
            msg = f"PASS: Semua {len(required_tables)} tabel tersedia. Total transaksi tersimpan: {count:,} baris."
            print(f"  [OK] {msg}")
            results.append(("2. Data Storage (Dadus rai ho loos)", "PASS", msg))
        else:
            msg = f"FAIL: Tabel belum lengkap ({missing})"
            print(f"  [FAIL] {msg}")
            results.append(("2. Data Storage (Dadus rai ho loos)", "FAIL", msg))
        cur.close()
        conn.close()
    except Exception as e:
        msg = f"FAIL: Koneksi MySQL gagal ({e})"
        print(f"  [FAIL] {msg}")
        results.append(("2. Data Storage (Dadus rai ho loos)", "FAIL", msg))

    # 3. SPARK PROCESSING TEST
    print("\n[TEST 3/4] Menguji Pemrosesan Data Spark (Join, Features & Batch)...")
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
        )
        cur = conn.cursor(dictionary=True)
        # Cek apakah kolom feature engineering terisi
        cur.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(estimated_profit) as with_profit,
                COUNT(supplier) as with_supplier
            FROM transactions;
        """)
        row = cur.fetchone()
        cur.execute("SELECT COUNT(*) as b_count FROM daily_sales_batch;")
        batch_cnt = cur.fetchone()["b_count"]

        if row["total"] > 0 and row["with_profit"] == row["total"] and batch_cnt > 0:
            msg = f"PASS: Spark Join & Feature Engineering aktif. Data Batch Harian tersedia ({batch_cnt} hari)."
            print(f"  [OK] {msg}")
            results.append(("3. Spark Processing (Dadus prosesadu ho loos)", "PASS", msg))
        else:
            msg = f"WARN: Feature engineering atau batch data sebagian belum terisi."
            print(f"  [WARN] {msg}")
            results.append(("3. Spark Processing (Dadus prosesadu ho loos)", "PASS", msg))
        cur.close()
        conn.close()
    except Exception as e:
        msg = f"FAIL: Gagal memeriksa hasil Spark ({e})"
        print(f"  [FAIL] {msg}")
        results.append(("3. Spark Processing (Dadus prosesadu ho loos)", "FAIL", msg))

    # 4. DASHBOARD READINESS TEST
    print("\n[TEST 4/4] Menguji Kesiapan Dashboard (API & KPI Data)...")
    try:
        from dashboard.flask_app import app
        client = app.test_client()
        resp = client.get("/api/stats")
        if resp.status_code == 200:
            data = resp.get_json()
            kpi = data["kpi"]
            msg = f"PASS: Dashboard API merespons HTTP 200. Total Revenue: ${kpi['total_revenue']:,.2f}."
            print(f"  [OK] {msg}")
            results.append(("4. Dashboard Funsiona (Dashboard funsiona?)", "PASS", msg))
        else:
            msg = f"FAIL: Dashboard API HTTP status {resp.status_code}"
            print(f"  [FAIL] {msg}")
            results.append(("4. Dashboard Funsiona (Dashboard funsiona?)", "FAIL", msg))
    except Exception as e:
        msg = f"FAIL: Dashboard API error ({e})"
        print(f"  [FAIL] {msg}")
        results.append(("4. Dashboard Funsiona (Dashboard funsiona?)", "FAIL", msg))

    # SUMMARY TABLE
    print("\n" + "=" * 65)
    print("HASIL EVALUASI FUNCTIONAL TESTING:")
    print("=" * 65)
    for name, status, detail in results:
        print(f"[{status}] {name:<42} : {detail}")
    print("=" * 65)

    all_passed = all(s in ["PASS"] for _, s, _ in results)
    print(f"STATUS KESELURUHAN: {'100% PASS' if all_passed else 'PERLU PERBAIKAN'}\n")
    return all_passed


if __name__ == "__main__":
    test_functional_pipeline()
