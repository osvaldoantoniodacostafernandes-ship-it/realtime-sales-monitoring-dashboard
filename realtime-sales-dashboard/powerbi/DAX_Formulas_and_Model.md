# Panduan Integrasi Microsoft Power BI Desktop & DAX Measures
**Big Data Capstone Project – S1 Siencia Komputador | DIT 2026**  
**Topik #11:** Real-Time Sales Dashboard (*Monitoring Venda no Revenue*)  
**Studi Kasus:** Toko Loilary Supermarket – Dili, Timor-Leste  

---

## 1. Koneksi Power BI ke Database MySQL
1. Buka aplikasi **Power BI Desktop**.
2. Pilih menu **Home** -> **Get Data** -> **MySQL database**.
3. Masukkan konfigurasi koneksi:
   * **Server:** `localhost:3306`
   * **Database:** `sales_dashboard`
   * **Data Connectivity mode:** Pilih **DirectQuery** (untuk real-time update langsung tanpa import ulang) atau **Import** (untuk performa visualisasi cepat).
4. Masukkan kredensial database:
   * **User:** `root`
   * **Password:** `dafa`
5. Pilih tabel-tabel utama berikut:
   * `transactions` (Tabel fakta penjualan kasir real-time & hasil feature engineering Spark)
   * `product_catalog` (Tabel dimensi produk & supplier master)
   * `sales_summary` (Tabel agregasi streaming window per 1 menit)
   * `category_summary` (Tabel ringkasan penjualan per kategori)
   * `payment_summary` (Tabel metode pembayaran)
   * `daily_sales_batch` (Hasil agregasi batch harian Spark)
   * `top_products_batch` (Ranking produk terlaris)
   * `ml_sales_forecast` (Hasil prediksi pendapatan Machine Learning)
   * `ml_anomaly_alerts` (Daftar transaksi anomali hasil deteksi)
   * `ml_product_clusters` (Segmentasi cluster produk K-Means)

---

## 2. Skema Relasi Data (Star Schema Model)
Pada tab **Model view** di Power BI, hubungkan relasi berikut:
* `product_catalog[product_name]` **1 ─── * (One-to-Many)** `transactions[product_name]`
* `ml_product_clusters[product_name]` **1 ─── 1 (One-to-One)** `product_catalog[product_name]`
* `transactions[transaction_id]` **1 ─── 1 (One-to-One)** `ml_anomaly_alerts[transaction_id]`

---

## 3. Rumus Formal DAX (Data Analysis Expressions)

Buat tabel khusus berlabel `_Measures` di Power BI dan masukkan formula DAX resmi berikut:

### 1. Total Revenue (Total Pendapatan Omzet)
```dax
Total Revenue = SUM(transactions[total_amount])
```

### 2. Total Transactions (Total Transaksi Kasir)
```dax
Total Transactions = COUNTROWS(transactions)
```

### 3. Total Units Sold (Total Kuantitas Barang)
```dax
Total Units Sold = SUM(transactions[quantity])
```

### 4. Average Order Value (AOV / Rata-rata Keranjang Belanja)
```dax
Average Order Value = 
DIVIDE([Total Revenue], [Total Transactions], 0)
```

### 5. Total Estimated Profit (Estimasi Keuntungan Bersih)
```dax
Total Estimated Profit = SUM(transactions[estimated_profit])
```

### 6. Profit Margin Percentage (Persentase Margin Keuntungan)
```dax
Profit Margin % = 
DIVIDE([Total Estimated Profit], [Total Revenue], 0)
```

### 7. High Value Transactions Count (Jumlah Transaksi Nilai Tinggi)
```dax
High Value Orders = 
CALCULATE(
    COUNTROWS(transactions),
    transactions[is_high_value] = 1
)
```

### 8. Total Anomalies Detected (Jumlah Anomali Terdeteksi)
```dax
Total Anomalies = COUNTROWS(ml_anomaly_alerts)
```

### 9. Moving Average Revenue (Rata-rata Bergerak 3 Periode)
```dax
Moving Avg Revenue 3D = 
AVERAGEX(
    DATESINPERIOD('daily_sales_batch'[report_date], LASTDATE('daily_sales_batch'[report_date]), -3, DAY),
    [Total Revenue]
)
```

### 10. Forecasted Revenue (Prediksi Pendapatan Masa Depan ML)
```dax
Forecasted Revenue = SUM(ml_sales_forecast[predicted_revenue])
```

---

## 4. Rekomendasi Tata Letak Visual Canvas Dashboard
* **Baris Atas (Header & KPI Cards):**
  * Card 1: `Total Revenue` (Format: `$#,##0.00`)
  * Card 2: `Total Transactions` (Format: `#,##0`)
  * Card 3: `Total Units Sold`
  * Card 4: `Average Order Value`
  * Card 5: `Total Anomalies` (Warna aksen merah)
* **Baris Tengah (Grafik Analitik):**
  * Line Chart: Tren Pendapatan Harian (`daily_sales_batch[report_date]` vs `[Total Revenue]`)
  * Clustered Column Chart: Omzet per Kategori Produk (`category_summary[category]` vs `[Total Revenue]`)
  * Donut Chart: Pangsa Pembayaran (`payment_summary[payment_method]` vs `[Total Revenue]`)
* **Baris Bawah (Detail & Alerts):**
  * Table / Matrix: Top 10 Produk Terlaris (`top_products_batch`)
  * Area Chart: Prediksi Masa Depan & Interval Kepercayaan (`ml_sales_forecast`)
  * Alert Feed Table: Transaksi Anomali Terbaru (`ml_anomaly_alerts`)
