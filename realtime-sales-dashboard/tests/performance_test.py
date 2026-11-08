# ==============================================================================
# tests/performance_test.py
# Pengujian Kinerja Sistem (Bab 14.B - Performance Testing)
#
# Memeriksa:
# 1. Processing Time (Waktu eksekusi pipeline & batch per batch transaksi)
# 2. Throughput (Kecepatan pemrosesan dalam records/detik)
# 3. Latency (Waktu respons end-to-end dari query & ingestion dalam milidetik)
# 4. Resource Utilization (Penggunaan CPU & Memory RAM)
# ==============================================================================

import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import psutil
import mysql.connector

from config.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)


def run_performance_testing():
    print("=" * 65)
    print("TESTING NO EVALUATION: B. PERFORMANCE TESTING (Bab 14.B)")
    print("Capstone Big Data DIT 2026 - Real-Time Sales Dashboard")
    print("=" * 65)

    # 1. RESOURCE UTILIZATION
    print("\n--- 1. Evaluasi Penggunaan Sumber Daya (CPU & RAM) ---")
    cpu_pct = psutil.cpu_percent(interval=1.0)
    ram = psutil.virtual_memory()
    ram_used_mb = ram.used / (1024 * 1024)
    ram_total_mb = ram.total / (1024 * 1024)
    ram_pct = ram.percent

    print(f"  • Penggunaan CPU (CPU Usage)      : {cpu_pct:.1f}%")
    print(f"  • Penggunaan RAM (Memory Usage)   : {ram_used_mb:,.0f} MB / {ram_total_mb:,.0f} MB ({ram_pct:.1f}%)")
    print(f"  • RAM Tersedia (Available RAM)   : {ram.available / (1024 * 1024):,.0f} MB")

    # 2. DATABASE QUERY & INGESTION LATENCY
    print("\n--- 2. Evaluasi Latensi Sistem (System Latency) ---")
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )
    cur = conn.cursor()

    latencies = []
    for _ in range(5):
        t0 = time.perf_counter()
        cur.execute("SELECT COUNT(*), SUM(total_amount) FROM transactions;")
        cur.fetchone()
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000)

    avg_latency = sum(latencies) / len(latencies)
    min_latency = min(latencies)
    max_latency = max(latencies)

    print(f"  • Latensi Query Rata-rata (Avg)   : {avg_latency:.2f} ms")
    print(f"  • Latensi Minimum (Min)           : {min_latency:.2f} ms")
    print(f"  • Latensi Maksimum (Max)          : {max_latency:.2f} ms")

    # 3. THROUGHPUT TESTING (RECORDS / SEC)
    print("\n--- 3. Evaluasi Throughput Pemrosesan (Throughput) ---")
    cur.execute("SELECT COUNT(*) FROM transactions;")
    total_txns = cur.fetchone()[0]

    # Uji pembacaan batch 10.000 records
    test_batch_size = min(10000, total_txns)
    t0 = time.perf_counter()
    cur.execute(f"SELECT * FROM transactions LIMIT {test_batch_size};")
    rows = cur.fetchall()
    t1 = time.perf_counter()

    read_duration = t1 - t0
    throughput = len(rows) / read_duration if read_duration > 0 else 0

    print(f"  • Ukuran Sampel Data Diuji        : {len(rows):,} records")
    print(f"  • Waktu Pemrosesan (Processing)   : {read_duration:.4f} detik")
    print(f"  • Kecepatan Throughput            : {throughput:,.0f} records / detik")

    cur.close()
    conn.close()

    # RINGKASAN TABEL KINERJA
    print("\n" + "=" * 65)
    print("TABEL RINGKASAN PERFORMANCE TESTING (Format Bab 14.B):")
    print("=" * 65)
    print(f"{'Metrik Pengujian':<30} | {'Nilai Terukur':<28}")
    print("-" * 65)
    print(f"{'Processing Time (10k records)':<30} | {f'{read_duration:.3f} detik':<28}")
    print(f"{'Throughput':<30} | {f'{throughput:,.0f} records/detik':<28}")
    print(f"{'Average End-to-End Latency':<30} | {f'{avg_latency:.2f} ms':<28}")
    print(f"{'CPU Utilization':<30} | {f'{cpu_pct:.1f}%':<28}")
    print(f"{'RAM Memory Utilization':<30} | {f'{ram_pct:.1f}% ({ram_used_mb:,.0f} MB)':<28}")
    print("=" * 65)


if __name__ == "__main__":
    run_performance_testing()
