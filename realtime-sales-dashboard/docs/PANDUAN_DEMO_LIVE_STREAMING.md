# 🚀 Panduan Demonstrasi Live Real-Time Pipeline Dili Multi-Sector

## 1. Mengapa Tadi MySQL Masih Muncul `0`?

Sebelumnya saat Anda mengecek `SELECT COUNT(*) FROM transactions;`, hasilnya masih `0` karena:
1. **Docker Desktop & Kafka sempat belum aktif / standby**: Producer sebelumnya hanya mencoba mengirim ke port 9092, dan jika broker belum siap maka tidak ada data yang terkirim.
2. **Producer aslinya tidak menyimpan langsung ke MySQL**: Arsitektur aslinya mewajibkan Spark Streaming berjalan bersamaan untuk menarik data dari Kafka ke MySQL. Jika Spark belum dijalankan atau ada jeda waktu, data di MySQL tetap `0`.

---

## 2. Peningkatan Baru yang Sudah Diterapkan (Dual-Stream Pipeline)

File [`producer/kafka_producer.py`](file:///c:/Users/LENOVO/Desktop/realtime-sales-dashboard/producer/kafka_producer.py) sekarang sudah **ditingkatkan secara otomatis**:
- ✅ **Kirim ke Kafka Broker**: Mengirim format JSON Semi-Structured ke topic `sales_transactions`.
- ✅ **Langsung Simpan ke MySQL (`transactions`)**: Setiap transaksi detik itu juga langsung tersimpan ke MySQL beserta feature engineering (*supplier, margin rate, profit, peak hour, bulk tier*).
- ✅ **Resilient & Anti-Crash**: Jika Kafka sedang proses sambung ulang, transaksi tetap tersimpan ke MySQL tanpa membuat terminal error.
- ✅ **Katalog 6 Sektor Toko di Dili**:
  1. `Timor Motor & Spareparts` (Comoro, Dili)
  2. `Dili Cosmetic & Beauty Store` (Colmera, Dili)
  3. `Timor Elektronik & Gadget` (Bebonuk, Dili)
  4. `Toko Loilary & Lita Supermarket` (Audian, Dili)
  5. `Centro Moda & Fashion Dili` (Pantai Kelapa, Dili)
  6. `Dili Stationery & Office Supply` (Mandarin, Dili)

---

## 3. Langkah Menjalankan Demo di Hadapan Dosen (Step-by-Step)

### Langkah 1: (Opsional) Kosongkan Data Menjadi 0
Jika ingin menunjukkan dashboard dari kondisi awal kosong (0) di hadapan dosen:
Buka **PowerShell** di folder project dan jalankan:
```powershell
python database/reset_live_transactions.py
```
> Hasil di terminal:
> `-> Live Transactions Count : 0 data`  
> `-> Target Big Data Dataset : 13,456 data`

---

### Langkah 2: Jalankan Producer Streaming
Buka terminal **PowerShell** dan jalankan:
```powershell
python producer/kafka_producer.py
```
Anda akan melihat data streaming mengalir detik demi detik:
```text
======================================================================
LIVE REAL-TIME STREAMING PIPELINE (KAFKA + MYSQL + POWER BI)
6 Sektor Komersial di Dili, Timor-Leste | Big Data Target: 13,456 Data
======================================================================
[STREAM #001] 16:15:02 | TRX-RET-... | Toko Loilary & Lita Super | Beras Premium 5kg Karung | Qty:3 | $ 19.50 | [Kafka: SENT | MySQL: SAVED]
[STREAM #002] 16:15:04 | TRX-MOT-... | Timor Motor & Spareparts  | Busi NGK Standar Motor   | Qty:4 | $ 12.00 | [Kafka: SENT | MySQL: SAVED]
[STREAM #003] 16:15:06 | TRX-MOD-... | Centro Moda & Fashion Di  | Kaos T-Shirt Polos Katun | Qty:1 | $  7.50 | [Kafka: SENT | MySQL: SAVED]
```
> Biarkan berjalan selama 15–30 detik (menghasilkan misalnya 15–25 transaksi), lalu tekan **Ctrl + C** untuk berhenti (atau biarkan terus mengalir).

---

### Langkah 3: Verifikasi di Terminal MySQL
Buka terminal MySQL Anda dan cek jumlah transaksi yang baru saja masuk:
```sql
USE sales_dashboard;
SELECT COUNT(*) FROM transactions;
```
Sekarang hasilnya **bukan 0 lagi**, melainkan sesuai jumlah streaming (contoh: `25`).

Cek 5 data transaksi kasir terbaru:
```sql
SELECT transaction_id, store_name, product_name, total_amount, payment_method 
FROM transactions 
ORDER BY id DESC 
LIMIT 5;
```

---

### Langkah 4: Tampilkan di Power BI Desktop
1. Buka file **Power BI Desktop** Anda.
2. Di pita menu paling atas (tab **Home**), klik tombol **Refresh**.
3. **Hasil yang langsung berubah di Dashboard**:
   - **Revenue**: Menampilkan total uang dari transaksi live (misal `$350.25`).
   - **Total Transactions**: Menampilkan jumlah live transaksi (misal `25`).
   - **Total Units Sold**: Total barang terjual (misal `58`).
   - **Avg. Order Value (AOV)**: Rata-rata per transaksi.
   - **Big Data Dataset Target**: Tetap menunjukkan target **`13,456`** (tanpa `$` dan tanpa `K`).
   - **Sales by Commercial Sector**: Grafik batang 6 toko langsung terisi warna.
   - **Sales by Product Category**: Donut chart kategori produk terisi.
   - **Latest Live Multi-Store Cashier Transactions**: Tabel kasir langsung terisi riwayat transaksi toko-toko di Dili.
