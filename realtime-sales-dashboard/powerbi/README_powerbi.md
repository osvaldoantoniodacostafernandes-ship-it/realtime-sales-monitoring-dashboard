# Panduan Lengkap Pembuatan Dashboard Power BI Desktop
**Big Data Capstone Project | Academic Year 2026**  
**Topik #11:** Real-Time Sales Dashboard (*Monitoring Venda no Revenue*)  
**Studi Kasus:** Toko Loilary Supermarket – Dili, Timor-Leste  

---

## Ringkasan Persiapan (Kondisi Laptop Anda Sudah Siap!)
* **MySQL Server:** Aktif di `localhost:3306` (Database: `sales_dashboard`).
* **Kredensial:** User `root`, Password `dafa`.
* **Konektor:** `MySQL Connector/NET 26.7.0` (Sudah terpasang di sistem Windows Anda).
* **Data:** **15.000 data transaksi** riil Toko Loilary (100% metode pembayaran Cash, mata uang USD) siap digunakan.

---

## TAHAP 1: Buka Power BI & Sambungkan ke Database MySQL

1. Buka aplikasi **Power BI Desktop** di laptop Anda.
2. Pada layar awal, klik menu **Get Data** (Dapatkan Data) di tab Ribbon atas, lalu pilih **MySQL database** (atau klik *More...* -> cari *MySQL* -> klik *Connect*).
3. Muncul jendela konfigurasi koneksi MySQL:
   * **Server:** Ketik `localhost:3306`
   * **Database:** Ketik `sales_dashboard`
   * **Data Connectivity mode:**
     * Pilih **DirectQuery** jika ingin dashboard meminta data real-time langsung ke database setiap kali visual di-refresh.
     * Atau pilih **Import** jika ingin memuat seluruh data ke dalam memori Power BI untuk visualisasi yang sangat responsif. *(Rekomendasi demo: Pilih **Import** untuk performa cepat dan stabil, atau **DirectQuery** jika dosen meminta pembuktian streaming langsung).*
   * Klik tombol **OK**.
4. Jika diminta memasukkan kredensial database (Database Credentials):
   * Pilih tab **Database** di sebelah kiri.
   * **User name:** `root`
   * **Password:** `dafa`
   * Klik **Connect**.

---

## TAHAP 2: Pilih Tabel Data (Navigator Window)

Pada jendela **Navigator**, beri tanda centang (checklist) pada tabel-tabel berikut:
1. `transactions` *(Tabel utama transaksi kasir & feature engineering)*
2. `product_catalog` *(Tabel master produk, supplier & margin toko Loilary)*
3. `category_summary` *(Tabel ringkasan penjualan per kategori)*
4. `payment_summary` *(Tabel ringkasan metode pembayaran)*
5. `daily_sales_batch` *(Tabel ringkasan batch harian & peak hour)*
6. `top_products_batch` *(Tabel ranking produk terlaris)*
7. `ml_sales_forecast` *(Tabel hasil prediksi pendapatan masa depan)*
8. `ml_anomaly_alerts` *(Tabel transaksi anomali hasil deteksi)*
9. `ml_product_clusters` *(Tabel segmentasi produk K-Means)*

Klik tombol **Load** di sudut kanan bawah. Tunggu beberapa detik hingga semua tabel selesai dimuat.

---

## TAHAP 3: Konfigurasi Relasi Model Data (Star Schema)

1. Di sisi paling kiri layar Power BI, klik ikon ketiga yaitu **Model view** (tampilan diagram relasi antar tabel).
2. Pastikan relasi tabel terhubung dengan benar:
   * Tarik garis dari kolom `product_catalog[product_name]` ke kolom `transactions[product_name]`. (Koneksi: **1 to Many**).
   * Tarik garis dari kolom `product_catalog[product_name]` ke kolom `ml_product_clusters[product_name]`. (Koneksi: **1 to 1**).
   * Tarik garis dari kolom `transactions[transaction_id]` ke kolom `ml_anomaly_alerts[transaction_id]`. (Koneksi: **1 to 1**).
3. Setelah relasi terbentuk, kembali ke tampilan kanvas visual dengan mengklik ikon pertama di sisi kiri: **Report view**.

---

## TAHAP 4: Membuat Ukuran Formal DAX (Measures)

Untuk mempermudah dan merapikan visual, buat tabel khusus ukuran (Measures):
1. Di tab **Home**, klik **Enter Data** (Masukkan Data). Beri nama tabel: `_Measures`, lalu klik **Load**.
2. Klik kanan pada tabel `_Measures` di panel *Data* sebelah kanan -> pilih **New measure** (Ukuran Baru).
3. Salin dan tempel formula DAX berikut satu per satu:

### 1. Total Pendapatan (Total Revenue)
```dax
Total Revenue = SUM(transactions[total_amount])
```

### 2. Total Transaksi (Total Transactions)
```dax
Total Transactions = COUNTROWS(transactions)
```

### 3. Total Unit Terjual (Total Units Sold)
```dax
Total Units Sold = SUM(transactions[quantity])
```

### 4. Rata-rata Nilai Keranjang Belanja (Average Order Value)
```dax
Average Order Value = DIVIDE([Total Revenue], [Total Transactions], 0)
```

### 5. Total Estimasi Keuntungan (Total Profit)
```dax
Total Estimated Profit = SUM(transactions[estimated_profit])
```

### 6. Persentase Margin Laba (Profit Margin %)
```dax
Profit Margin % = DIVIDE([Total Estimated Profit], [Total Revenue], 0)
```

### 7. Total Anomali Terdeteksi (Total Anomalies)
```dax
Total Anomalies = COUNTROWS(ml_anomaly_alerts)
```

*(Ubah format angka pada menu **Measure tools**: ubah `Total Revenue`, `Average Order Value`, dan `Total Estimated Profit` menjadi mata uang **$ (USD)**).*

---

## TAHAP 5: Mendesain Kanvas Dashboard Visual

Susun kanvas halaman Power BI Anda sesuai layout profesional berikut:

### 1. Header Judul (Atas)
* Klik tab **Insert** -> pilih **Text box**.
* Ketik: `TOKO LOILARY - REAL-TIME SALES MONITORING DASHBOARD`
* Subtitle: `Big Data Capstone Project | Dili Institute of Technology (DIT) 2026`
* Ukuran font 18pt, bold, warna biru tua (#1E3A8A).

### 2. Baris Kartu Indikator Utama (KPI Cards)
Tambahkan 5 visual bertipe **Card** (atau *New Card*):
* **Card 1:** Masukkan measure `[Total Revenue]`. Beri label: *Total Revenue*.
* **Card 2:** Masukkan measure `[Total Transactions]`. Beri label: *Total Transaksi*.
* **Card 3:** Masukkan measure `[Total Units Sold]`. Beri label: *Unit Terjual*.
* **Card 4:** Masukkan measure `[Average Order Value]`. Beri label: *Rata-rata Order (AOV)*.
* **Card 5:** Masukkan measure `[Total Anomalies]`. Ubah warna font menjadi merah (#EF4444) sebagai indikator alert.

### 3. Grafik Tren Penjualan Harian & Jam Sibuk (Tengah Kiri)
* Pilih visual **Line and Clustered Column Chart** atau **Line Chart**.
* **X-axis:** Masukkan `daily_sales_batch[report_date]`.
* **Y-axis:** Masukkan `[Total Revenue]`.
* Beri judul: *Tren Penjualan Harian Toko Loilary ($)*.

### 4. Grafik Penjualan per Kategori Produk (Tengah Kanan)
* Pilih visual **Clustered Bar Chart** (Grafik Batang Horizontal).
* **Y-axis:** Masukkan `category_summary[category]`.
* **X-axis:** Masukkan `category_summary[total_revenue]`.
* Aktifkan opsi **Data labels** (on) agar nilai dollar terlihat jelas di ujung setiap batang.

### 5. Komposisi Metode Pembayaran (Bawah Kiri)
* Pilih visual **Donut Chart**.
* **Legend:** Masukkan `payment_summary[payment_method]`.
* **Values:** Masukkan `payment_summary[total_revenue]`.
* Beri judul: *Distribusi Metode Pembayaran*.

### 6. Tabel Top 10 Produk Terlaris (Bawah Tengah)
* Pilih visual **Table** (Tabel).
* Masukkan kolom:
  1. `top_products_batch[sales_rank]` (Peringkat)
  2. `top_products_batch[product_name]` (Nama Produk)
  3. `top_products_batch[category]` (Kategori)
  4. `top_products_batch[total_units_sold]` (Unit Terjual)
  5. `top_products_batch[total_revenue]` (Total Revenue $)
* Urutkan tabel berdasarkan `sales_rank` (Ascending: 1 s/d 10).

### 7. Visualisasi Prediksi Masa Depan Machine Learning (Bawah Kanan)
* Pilih visual **Area Chart** atau **Line Chart**.
* **X-axis:** Masukkan `ml_sales_forecast[forecast_time]`.
* **Y-axis:** Masukkan `ml_sales_forecast[predicted_revenue]`.
* Beri judul: *Prediksi Penjualan 6 Jam ke Depan (ML Regression)*.

---

## TAHAP 6: Mengatur Pembaruan Otomatis (Page Refresh)
Jika menggunakan mode **DirectQuery**:
1. Klik area kosong di kanvas laporan.
2. Di panel **Format page** sebelah kanan, buka bagian **Page refresh**.
3. Aktifkan (toggle ON) **Page refresh**, lalu atur durasi refresh menjadi **5 seconds** atau **10 seconds**.
4. Power BI akan melakukan refresh otomatis secara langsung saat Kafka producer dan Spark streaming mengirimkan data baru ke MySQL!

---

## TAHAP 7: Simpan File Proyek Power BI (.pbix)

1. Klik menu **File** -> **Save As**.
2. Simpan file di dalam folder proyek Anda:
   * **Lokasi:** `C:\Users\LENOVO\Desktop\realtime-sales-dashboard\powerbi\`
   * **Nama File:** `sales_dashboard.pbix`
3. Klik **Save**.
4. File `.pbix` Anda sekarang siap ditunjukkan kepada dosen penguji saat sesi demonstrasi langsung!
