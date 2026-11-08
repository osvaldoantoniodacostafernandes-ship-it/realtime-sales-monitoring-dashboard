# System Architecture & Data Flow Documentation
**Big Data Capstone Project – S1 Siencia Komputador | DIT 2026**  
**Topik #11:** Real-Time Sales Dashboard (*Monitoring Venda no Revenue*)  
**Studi Kasus:** Toko Loilary Supermarket – Dili, Timor-Leste  

---

## 1. System Architecture Diagram (Bab 7 Manual)

```mermaid
flowchart TD
    subgraph Data_Source ["1. DATA SOURCE (Toko Loilary Dili)"]
        DS1["Product Master Lapangan<br/>(158 Produk Dili Timor-Leste)"]
        DS2["POS Cashier Simulator<br/>(producer/kafka_producer.py)"]
        DS1 --> DS2
    end

    subgraph Data_Ingestion ["2. DATA INGESTION (Apache Kafka 7.5.0)"]
        K1["Kafka Broker (localhost:9092)"]
        K2["Topic: sales_transactions<br/>(Semi-Structured JSON)"]
        DS2 -- "JSON Event Stream (0.5s - 2.0s)" --> K1
        K1 --> K2
    end

    subgraph Data_Processing ["3. DISTRIBUTED DATA PROCESSING (Apache Spark 3.5.0)"]
        SP1["Spark Structured Streaming<br/>(spark/spark_streaming_job.py)"]
        SP2["Spark Batch Processing<br/>(spark/spark_batch_job.py)"]
        PC["Broadcast Product Catalog<br/>(JOIN Metadata)"]
        
        K2 -- "Streaming Read" --> SP1
        PC -- "Broadcast JOIN" --> SP1
        SP1 -- "Feature Engineering<br/>(Profit, Margin, Peak Hour, High Value)" --> SP1
        SP1 -- "Window Aggregation (1-min tumble)" --> SP1
    end

    subgraph Data_Storage ["4. DATA STORAGE (MySQL 8.0: sales_dashboard)"]
        DB1[("transactions<br/>(Raw & Enriched)")]
        DB2[("sales_summary<br/>(1-min Window)")]
        DB3[("category_summary<br/>(Per Category)")]
        DB4[("payment_summary<br/>(Payment Method)")]
        DB5[("product_catalog<br/>(158 Products Metadata)")]
        DB6[("daily_sales_batch<br/>(Daily Aggregations)")]
        DB7[("top_products_batch<br/>(Product Rankings)")]
        DB8[("ml_sales_forecast<br/>(6-Hour Forecast)")]
        DB9[("ml_anomaly_alerts<br/>(Z-Score Outliers)")]
        DB10[("ml_product_clusters<br/>(K-Means Clusters)")]

        SP1 -- "JDBC Append" --> DB1
        SP1 -- "JDBC Append" --> DB2
        SP1 -- "JDBC Append" --> DB3
        SP1 -- "JDBC Append" --> DB4
        SP2 -- "JDBC Overwrite" --> DB6
        SP2 -- "JDBC Overwrite" --> DB7
        DB1 -- "Historical Read" --> SP2
    end

    subgraph Data_Analytics ["5. BIG DATA ANALYTICS & MACHINE LEARNING (Bab 5.5)"]
        ML1["Sales & Revenue Forecasting<br/>(Multivariate OLS Regression)"]
        ML2["Real-Time Anomaly Detection<br/>(Z-Score & IQR Outliers)"]
        ML3["Product Segmentation<br/>(K-Means Clustering)"]
        
        DB1 -- "Pandas / NumPy Batch" --> ML1
        DB1 -- "Statistical Outlier Filter" --> ML2
        DB1 -- "Z-Score Cluster Matrix" --> ML3
        
        ML1 --> DB8
        ML2 --> DB9
        ML3 --> DB10
    end

    subgraph Visualization ["6. REAL-TIME VISUALIZATION & DASHBOARD (Bab 5.6)"]
        D1["Streamlit Web Dashboard<br/>(dashboard/app.py @ port 8501)"]
        D2["Flask Interactive Dashboard<br/>(dashboard/flask_app.py @ port 5000)"]
        D3["Microsoft Power BI Desktop<br/>(DirectQuery Star Schema)"]

        DB1 & DB2 & DB3 & DB4 & DB6 & DB7 & DB8 & DB9 & DB10 --> D1
        DB1 & DB2 & DB3 & DB4 & DB8 & DB9 --> D2
        DB1 & DB2 & DB3 & DB4 & DB5 & DB6 & DB7 & DB8 & DB9 --> D3
    end
```

---

## 2. Data Flow Diagram (DFD)

### DFD Level 0 (Context Diagram)
```mermaid
flowchart LR
    Kasir["Kasir Toko Loilary<br/>(Dili, Timor-Leste)"] -- "Transaksi Penjualan (JSON)" --> Sistem["Sistem Big Data Real-Time Sales Dashboard"]
    Sistem -- "KPI Omzet, Tren Real-Time, Peringatan Anomali" --> Manajer["Manajer Toko & Pemilik Bisnis"]
    Sistem -- "Laporan Analisis Penjualan & Prediksi Masa Depan" --> TimAnalis["Tim Analis Bisnis"]
```

### DFD Level 1 (Proses Terperinci)
```mermaid
flowchart TD
    P1["1.0 Pengumpulan & Ingesti Data<br/>(kafka_producer.py -> Kafka)"]
    P2["2.0 Pembersihan, Join & Rekayasa Fitur<br/>(spark_streaming_job.py)"]
    P3["3.0 Pemrosesan Batch Historis<br/>(spark_batch_job.py)"]
    P4["4.0 Analitik Prediktif & Deteksi Anomali<br/>(sales_ml_analytics.py)"]
    P5["5.0 Visualisasi Real-Time & Notifikasi<br/>(Streamlit / Flask / Power BI)"]

    Kasir["Kasir Lapangan"] --> P1
    P1 --> KTopic[("Queue: Kafka Topic")]
    KTopic --> P2
    Catalog[("Data Store: product_catalog")] --> P2
    P2 --> DBMain[("Data Store: MySQL Database")]
    DBMain --> P3
    P3 --> DBMain
    DBMain --> P4
    P4 --> DBMain
    DBMain --> P5
    P5 --> DashboardUI["Dashboard Monitor Toko"]
```

---

## 3. Pemetaan Konsep 5V Big Data (Bab 3 Manual)

| Dimensi 5V | Implementasi Nyata pada Proyek Ini |
|---|---|
| **Volume** | Sistem mengelola dataset transaksi berkelanjutan (15.000 transaksi di MySQL, 100% Cash dari data riil Toko Loilary) dan telah lulus uji skalabilitas linier hingga 1.000.000 records. |
| **Velocity** | Data transaksi dihasilkan kasir dan dialirkan ke Kafka setiap 0.5 – 2 detik, diproses dengan Spark Streaming dalam micro-batch window 1 menit. |
| **Variety** | Mengolah format data semi-structured JSON bertingkat (nested store, product, payment) dan ditransformasi menjadi format relational structured schema. |
| **Veracity** | Memastikan keakuratan data melalui validasi skema JSON, filter null/missing values, dan deteksi anomali harga/kuantitas menggunakan Statistical Z-Score dan IQR. |
| **Value** | Mengubah jutaan angka transaksi mentah menjadi metrik bisnis strategis: estimasi margin laba bersih, jam puncak belanja (peak hour), ranking produk terlaris, dan prediksi pendapatan 6 jam ke depan. |
