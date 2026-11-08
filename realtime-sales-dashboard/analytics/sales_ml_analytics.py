# ==============================================================================
# analytics/sales_ml_analytics.py
# Big Data Analytics & Machine Learning Module: K-Means Product Segmentation
# Dili Multi-Sector Retail Dashboard (Timor-Leste)
# ==============================================================================
# Fokus Utama:
# - CLUSTERING (K-Means Clustering, k=3)
# - Segmentasi Portofolio Produk Berdasarkan 4 Fitur:
#   1. total_units    (Total volume produk terjual)
#   2. total_revenue  (Total omset/pendapatan)
#   3. total_orders   (Frekuensi transaksi)
#   4. avg_unit_price (Rata-rata harga satuan)
#
# Output:
# - Tabel MySQL: `ml_product_clusters`
# ==============================================================================

import os
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd
import mysql.connector

from config.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)


def get_db_connection():
    """Koneksi ke database MySQL."""
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


def run_kmeans_clustering():
    print("=" * 70)
    print("ANALITIKA BIG DATA: SEGMENTASAUN PRODUTU K-MEANS (k=3)")
    print("Setor Komersial 6 iha Dili, Timor-Leste")
    print("=" * 70)

    # 1. Muat Data Transaksi (Dari MySQL atau CSV Master)
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM transactions;")
    trx_count = cur.fetchone()[0]

    csv_path = PROJECT_ROOT / "data" / "transactions_multi_store_12531.csv"
    if not csv_path.exists():
        csv_path = PROJECT_ROOT / "data" / "transactions_multi_store_13456.csv"
    if trx_count >= 50:
        print(f"[INFO] Lee dadus transasaun {trx_count:,} hosi tabela MySQL 'transactions'...")
        df = pd.read_sql("SELECT product_name, category, quantity, price, total_amount, transaction_id FROM transactions;", conn)
    elif csv_path.exists():
        print(f"[INFO] Lee dadus kompletu 12.531 transasaun hosi '{csv_path.name}'...")
        df = pd.read_csv(csv_path)
    else:
        print("[ERRO] La iha dadus transasaun atu halo clustering.")
        cur.close()
        conn.close()
        return

    # 2. Agregasi Level Produk (Product Performance Profile)
    prod_summary = (
        df.groupby(["product_name", "category"])
        .agg(
            total_units=("quantity", "sum"),
            total_revenue=("total_amount", "sum"),
            total_orders=("transaction_id", "count"),
            avg_unit_price=("price", "mean")
        )
        .reset_index()
    )

    print(f"[INFO] Hetan produtu uniku {len(prod_summary)} hosi setor retallu 6 iha Dili.")

    # 3. Fitur untuk Clustering & Standarisasi (Z-Score Normalization)
    features = ["total_units", "total_revenue", "total_orders", "avg_unit_price"]
    raw_data = prod_summary[features].astype(float).values

    mean_vals = np.mean(raw_data, axis=0)
    std_vals = np.std(raw_data, axis=0) + 1e-6
    scaled_data = (raw_data - mean_vals) / std_vals

    # 4. Implementasi Algoritma K-Means Clustering (k=3)
    k = 3
    np.random.seed(42)
    initial_indices = np.random.choice(len(scaled_data), k, replace=False)
    centroids = scaled_data[initial_indices].copy()

    cluster_indices = np.zeros(len(scaled_data), dtype=int)
    for iteration in range(35):
        # Hitung jarak Euclidean ke setiap centroid
        distances = np.linalg.norm(scaled_data[:, None] - centroids, axis=2)
        cluster_indices = np.argmin(distances, axis=1)

        # Perbarui posisi centroid
        new_centroids = np.array([
            scaled_data[cluster_indices == i].mean(axis=0) if np.sum(cluster_indices == i) > 0 else centroids[i]
            for i in range(k)
        ])

        if np.allclose(centroids, new_centroids, atol=1e-4):
            break
        centroids = new_centroids

    prod_summary["cluster_id"] = cluster_indices

    # 5. Profiling & Pelabelan Klaster Berdasarkan Karakteristik Bisnis
    c_means = prod_summary.groupby("cluster_id")[["total_revenue", "total_units", "avg_unit_price"]].mean()
    max_rev_id = c_means["total_revenue"].idxmax()
    max_units_id = c_means["total_units"].idxmax()

    label_map = {
        max_rev_id: "Top Contributor (High Revenue)",
    }
    for cid in range(k):
        if cid not in label_map:
            if cid == max_units_id:
                label_map[cid] = "Fast-Moving Goods"
            else:
                label_map[cid] = "Standard Steady Goods"

    # Pastikan semua k memiliki label
    if len(label_map) < k:
        for cid in range(k):
            if cid not in label_map:
                label_map[cid] = "Standard Steady Goods"

    prod_summary["cluster_label"] = prod_summary["cluster_id"].map(label_map)

    # 6. Simpan Hasil Segmentasi ke MySQL `ml_product_clusters`
    cur.execute("TRUNCATE TABLE ml_product_clusters;")
    for _, row in prod_summary.iterrows():
        cur.execute("""
            INSERT INTO ml_product_clusters 
                (product_name, category, total_units, total_revenue, cluster_id, cluster_label)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, (
            str(row["product_name"]),
            str(row["category"]),
            int(row["total_units"]),
            round(float(row["total_revenue"]), 2),
            int(row["cluster_id"]),
            str(row["cluster_label"])
        ))

    conn.commit()
    cur.close()
    conn.close()

    # 7. Tampilkan Ringkasan Hasil Segmentasi
    print("\n" + "=" * 70)
    print("REZULTADU SEGMENTASAUN PRODUTU (K-MEANS CLUSTERING):")
    print("=" * 70)
    summary_report = prod_summary.groupby("cluster_label").agg(
        jumlah_produk=("product_name", "count"),
        total_unit_terjual=("total_units", "sum"),
        total_omset=("total_revenue", "sum"),
        rata_rata_harga=("avg_unit_price", "mean")
    ).round(2)

    for label, r in summary_report.iterrows():
        print(f"\n[KLASTER] {label}:")
        print(f"  -> Total Produtu        : {int(r['jumlah_produk'])} produtu")
        print(f"  -> Total Unidade Fa'an : {int(r['total_unit_terjual']):,} units")
        print(f"  -> Total Reseita       : ${r['total_omset']:,.2f}")
        print(f"  -> Rata-rata Folin     : ${r['rata_rata_harga']:,.2f}")

    print("\n" + "=" * 70)
    print("[SUSESU] Dadus segmentasaun K-Means rai ona iha MySQL: `ml_product_clusters`.")
    print("Loke Power BI Desktop no hanehan 'Refresh' atu haree vizualizasaun clustering.")
    print("=" * 70)


if __name__ == "__main__":
    run_kmeans_clustering()
