# Real-Time Sales Monitoring Dashboard
**Big Data Capstone Project | Academic Year 2026**  
**Program Studi:** S1 Siencia Komputador / Ilmu Komputer – Dili Institute of Technology (DIT)  
**Topik #11:** Real-Time Sales Dashboard (*Monitoring Venda no Revenue*)  
**Studi Kasus:** Toko Loilary Supermarket – Dili, Timor-Leste  
**Teknologi:** Apache Kafka 7.5.0, Apache Spark 3.5.0 (Structured Streaming & Batch), MySQL 8.0, Streamlit, Flask, Microsoft Power BI Desktop, Python 3.11  

---

## 1. Arsitektur & Alur Pipeline Sistem (100% Big Data Pipeline)

```
[Product Master Lapangan Dili] -> [producer/kafka_producer.py]
                                         |
                                         v (Format: Semi-Structured JSON)
                         [Apache Kafka Broker] (Topic: sales_transactions)
                                         |
                    +--------------------+--------------------+
                    |                                         |
                    v (Streaming Micro-Batch)                 v (Historical Batch Read)
[spark/spark_streaming_job.py]                  [spark/spark_batch_job.py]
  - Data Cleaning & Schema Validation             - Daily Sales Aggregation
  - Broadcast JOIN with product_catalog           - Top 10 Best Sellers Ranking
  - Feature Engineering (Profit, Margin, Peak)    - Peak Shopping Hour Discovery
                    |                                         |
                    +--------------------+--------------------+
                                         |
                                         v (JDBC Structured Writes)
                         [Database MySQL 8.0: sales_dashboard]
                           - transactions (Raw & Enriched)
                           - sales_summary (Window 1 menit)
                           - category_summary (Per Kategori)
                           - payment_summary (Metode Pembayaran)
                           - product_catalog (158 Produk Dili)
                           - daily_sales_batch (Ringkasan Harian)
                           - top_products_batch (Ranking Produk)
                           - ml_sales_forecast (Prediksi Pendapatan)
                           - ml_anomaly_alerts (Peringatan Anomali)
                           - ml_product_clusters (Segmentasi K-Means)
                                         |
                    +--------------------+--------------------+
                    |                    |                    |
                    v                    v                    v
          [Streamlit Dashboard]   [Flask Web App]      [Power BI Desktop]
           `dashboard/app.py`   `dashboard/flask_app.py`  (DirectQuery Mode)
```

---

## 2. Struktur File Project Lengkap

```text
realtime-sales-dashboard/
│
├── analytics/
│   └── sales_ml_analytics.py   # Modul Machine Learning: Regression, Anomaly Detection, K-Means (Bab 5.5)
│
├── config/
│   ├── config.py               # Konfigurasi terpusat: Kafka, MySQL, HADOOP_HOME & tuning memori JVM
│   ├── db_config.py            # Kredensial MySQL
│   └── kafka_config.py         # Kredensial Kafka
│
├── dashboard/
│   ├── app.py                  # Real-Time Interactive Dashboard (Streamlit - Port 8501)
│   ├── flask_app.py            # Real-Time Interactive Web Server (Flask - Port 5000)
│   └── templates/
│       └── index.html          # Frontend responsive Chart.js & Tailwind CSS auto-refresh
│
├── database/
│   ├── init_complete_schema.py # Inisialisasi otomatis seluruh 10 tabel & katalog produk
│   └── setup_database.sql      # DDL SQL skema basis data
│
├── docker/
│   └── docker-compose.yml      # Container Zookeeper, Kafka broker, dan Kafka-UI
│
├── docs/
│   └── SYSTEM_ARCHITECTURE.md  # Diagram Arsitektur Sistem resmi & Data Flow Diagram (DFD L0 & L1)
│
├── powerbi/
│   ├── DAX_Formulas_and_Model.md # Panduan Power BI Desktop, Star Schema, & 10 Rumus DAX
│   └── README_powerbi.md
│
├── producer/
│   ├── data/
│   │   └── product_master_lapangan_cleaned.csv  # 158 data produk toko Loilary Dili
│   └── kafka_producer.py       # Producer pengirim transaksi semi-structured JSON
│
├── spark/
│   ├── jars/                   # Driver MySQL Connector JDBC
│   ├── spark_streaming_job.py  # Spark Streaming + JOIN Catalog + Feature Engineering (Bab 5.4)
│   ├── spark_batch_job.py      # Spark Batch Processing agregasi harian & ranking produk (LO5)
│   ├── test_kafka_spark.py     # Uji koneksi Spark ke Kafka
│   └── test_mysql.py           # Uji koneksi Spark ke MySQL
│
├── tests/                      # 4 Rangkaian Testing & Evaluation (Bab 14 Manual)
│   ├── functional_test.py      # Bab 14.A: Functional Testing (Ingestion -> Spark -> MySQL -> Web)
│   ├── performance_test.py     # Bab 14.B: Performance Testing (Throughput, Latency, CPU/RAM)
│   ├── data_quality_test.py    # Bab 14.C: Data Quality Testing (Missing, Duplicates, Outliers)
│   └── scalability_test.py     # Bab 14.D: Scalability Testing (10k, 50k, 100k, 500k, 1M records)
│
├── requirements.txt            # Dependensi Python
├── run_system_check.py         # Master Health Check & 100% Compliance Evaluator
└── README.md                   # Dokumentasi lengkap proyek
```

---

## 3. Cara Menjalankan Seluruh Proyek (Panduan Lengkap)

### Langkah 1: Jalankan Apache Kafka (Docker)
Buka terminal PowerShell:
```bash
cd C:\Users\LENOVO\Desktop\realtime-sales-dashboard\docker
cd .\docker
Get-ChildItem
docker compose up -d
docker ps
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```
*(Buka browser di `http://localhost:8080` untuk melihat Kafka UI).*

### Langkah 2: Inisialisasi Skema Database Lengkap
Buka terminal di folder root project:
```bash
cd C:\Users\LENOVO\Desktop\realtime-sales-dashboard
python database/init_complete_schema.py
```

### Langkah 3: Jalankan Kafka Producer (Aliran Transaksi Toko Loilary)
```bash
python producer/kafka_producer.py
```
*Transaksi JSON dikirimkan ke Kafka secara terus menerus setiap 0.5 – 2 detik.*

### Langkah 4: Jalankan Spark Structured Streaming (Join & Feature Engineering)
Buka terminal baru:
```bash
python spark/spark_streaming_job.py
```

### Langkah 5: Jalankan Spark Batch Processing
Buka terminal baru:
```bash
python spark/spark_batch_job.py
```

### Langkah 6: Jalankan Modul Machine Learning & Analytics
```bash
python analytics/sales_ml_analytics.py
```

### Langkah 7: Buka Real-Time Web Dashboard

Pilih salah satu dashboard web sesuai preferensi:

* **Opsi A (Streamlit Interactive Dashboard):**
  ```bash
  python -m streamlit run dashboard/app.py
  ```
  *Buka browser di `http://localhost:8501`.*

* **Opsi B (Flask Lightweight Web Dashboard):**
  ```bash
  python dashboard/flask_app.py
  ```
  *Buka browser di `http://localhost:5000`.*

---

## 4. Menjalankan Master Verification (Audit 100% Kepatuhan)

Untuk memverifikasi seluruh komponen pengujian secara otomatis sekaligus:
```bash
python run_system_check.py
```

Skrip ini akan menguji dan menampilkan hasil dari 4 aspek evaluasi resmi Bab 14:
* **Functional Testing:** Verifikasi konektivitas Kafka, Spark, MySQL, dan Dashboard API.
* **Data Quality Testing:** Verifikasi 0 missing values, 0 duplikasi, dan validitas integritas data.
* **Performance Testing:** Mengukur Throughput (~100.000 rec/s), Latensi rata-rata (~7 ms), dan utilisasi CPU/RAM.
* **Scalability Testing:** Evaluasi linier dari 10.000 hingga 1.000.000 records.
