# ==============================================================================
# kafka_producer.py
# Real-Time Multi-Sector Sales Data Producer & Real-Time Pipeline Streamer
# Dili, Timor-Leste Commercial Retail Sectors (13,456 Multi-Sector Catalog)
# ==============================================================================

import json
import random
import sys
import time
import uuid
from datetime import datetime
from pathlib import Path

# ------------------------------------------------------------------------------
# PROJECT PATH & CONFIG
# ------------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    PRODUCER_DELAY_MIN,
    PRODUCER_DELAY_MAX,
    PAYMENT_METHODS,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)

import mysql.connector
from kafka import KafkaProducer

# ------------------------------------------------------------------------------
# 6 TOKO REALISTIS BERAGAM SEKTOR KOMERSIAL DI DILI
# ------------------------------------------------------------------------------
STORES_CATALOG = [
    {
        "name": "Toko Loilary",
        "sector": "Supermarket & Sembako",
        "location": "Audian, Dili",
        "prefix": "LOI",
        "products": [
            ("Beras Premium 5kg Karung", "Food & Groceries", 6.50, "Timor Food Supplier Lda", 0.15),
            ("Minyak Goreng Sawit 2 Liter", "Food & Groceries", 3.75, "Timor Food Supplier Lda", 0.18),
            ("Indomie Goreng (Pack 5)", "Food & Groceries", 2.25, "Timor Food Supplier Lda", 0.22),
            ("Telur Ayam 1 Rak (30 Butir)", "Food & Groceries", 5.50, "Dili Poultry Supply Lda", 0.18),
            ("Gula Pasir Putih 1kg", "Food & Groceries", 1.35, "Timor Food Supplier Lda", 0.18),
            ("Kopi Bubuk Timor Dili 250g", "Beverages", 3.50, "Timor Heritage Coffee", 0.35),
            ("Sabun Cuci Piring Sunlight 750ml", "Household Goods", 2.10, "Dili Consumer Goods Lda", 0.28),
            ("Deterjen Bubuk Daia 850g", "Household Goods", 2.25, "Dili Consumer Goods Lda", 0.25),
            ("Biskuit Khong Guan 650g", "Food & Groceries", 5.50, "Timor Food Supplier Lda", 0.25),
            ("Teh Celup SariWangi Kotak", "Beverages", 1.50, "Timor Food Supplier Lda", 0.30),
            ("Tepung Terigu Segitiga Biru 1kg", "Food & Groceries", 1.25, "Timor Food Supplier Lda", 0.20),
            ("Garam Dapur Yodium 500g", "Food & Groceries", 0.75, "Timor Food Supplier Lda", 0.25),
        ]
    },
    {
        "name": "Lita Supermarket",
        "sector": "Supermarket Modern",
        "location": "Mandarin, Dili",
        "prefix": "LIT",
        "products": [
            ("Susu Ultra Milk 1 Liter UHT", "Food & Groceries", 2.00, "Comoro Dairy Supply", 0.22),
            ("Coca-Cola Can 330ml", "Beverages", 0.75, "Dili Beverage Distribution", 0.35),
            ("Air Mineral Aqua Fresca 1.5L", "Beverages", 1.00, "Dili Beverage Distribution", 0.40),
            ("Cokelat SilverQueen 62g", "Food & Groceries", 1.85, "Timor Food Supplier Lda", 0.28),
            ("Keju Kraft Cheddar 165g", "Food & Groceries", 2.40, "Comoro Dairy Supply", 0.25),
            ("Jus Buah Buavita 1 Liter", "Beverages", 2.20, "Dili Beverage Distribution", 0.30),
            ("Roti Tawar Gandum Sari Roti", "Food & Groceries", 1.75, "Timor Bakery Supply", 0.22),
            ("Sereal Koko Krunch 330g", "Food & Groceries", 3.60, "Timor Food Supplier Lda", 0.25),
            ("Daging Sapi Beku Impor 500g", "Food & Groceries", 5.75, "Dili Cold Storage Lda", 0.20),
            ("Ikan Salmon Fillet Beku 300g", "Food & Groceries", 7.50, "Dili Cold Storage Lda", 0.22),
            ("Sabun Mandi Lifebuoy Cair 450ml", "Household Goods", 3.25, "Dili Consumer Goods Lda", 0.28),
            ("Shampo Sunsilk Black Shine 320ml", "Household Goods", 4.10, "Dili Consumer Goods Lda", 0.28),
        ]
    },
    {
        "name": "Bengkel Motor Dili",
        "sector": "Bengkel Servis Motor",
        "location": "Comoro, Dili",
        "prefix": "BEN",
        "products": [
            ("Oli Mesin Yamalube 4T 800ml", "Motor & Spareparts", 6.50, "Yamaha Timor Distributor Lda", 0.28),
            ("Oli Mesin Castrol Power1 1L", "Motor & Spareparts", 7.75, "Castrol Timor Lda", 0.25),
            ("Busi NGK Standar Motor", "Motor & Spareparts", 3.00, "Timor Auto Parts Lda", 0.35),
            ("Kampas Rem Depan / Belakang", "Motor & Spareparts", 5.50, "Timor Auto Parts Lda", 0.32),
            ("Ban Luar Tubeless FDR 80/90-14", "Motor & Spareparts", 18.50, "Dili Tyre Supply Lda", 0.22),
            ("Ban Dalam Swallow 2.75-17", "Motor & Spareparts", 4.25, "Dili Tyre Supply Lda", 0.25),
            ("Rantai Roda & Gir Set Honda/Yamaha", "Motor & Spareparts", 16.00, "Timor Auto Parts Lda", 0.25),
            ("Spion Standar Motor (Sepasang)", "Motor & Spareparts", 4.75, "Timor Auto Parts Lda", 0.35),
            ("Lampu Bohlam LED Motor H6", "Motor & Spareparts", 3.50, "Timor Auto Parts Lda", 0.35),
            ("Filter Udara Motor Matic", "Motor & Spareparts", 4.00, "Timor Auto Parts Lda", 0.30),
            ("Minyak Rem DOT 3 Jumbo", "Motor & Spareparts", 2.50, "Castrol Timor Lda", 0.30),
            ("Jasa Servis Ringan & Tune Up Motor", "Motor & Spareparts", 5.00, "Bengkel Motor Dili", 0.60),
            ("Jasa Ganti Oli & Pemeriksaan", "Motor & Spareparts", 2.00, "Bengkel Motor Dili", 0.70),
        ]
    },
    {
        "name": "Dealer Motor & Mobil Dili",
        "sector": "Dealer Kendaraan & Sparepart",
        "location": "Comoro, Dili",
        "prefix": "DEA",
        "products": [
            ("Aki Kering GS Astra 12V 5Ah Motor", "Motor & Spareparts", 22.00, "Astra Dili Battery Supply", 0.20),
            ("Helm Standar SNI Half Face", "Motor & Spareparts", 19.50, "Timor Safety Gear Lda", 0.30),
            ("Helm Full Face Touring", "Motor & Spareparts", 38.00, "Timor Safety Gear Lda", 0.28),
            ("Gembok Pengaman Cakram Motor", "Motor & Spareparts", 6.00, "Timor Safety Gear Lda", 0.35),
            ("Cover Body Motor Honda/Yamaha", "Motor & Spareparts", 28.00, "Timor Auto Parts Lda", 0.25),
            ("Aki Mobil Incoe Maintenance Free 45Ah", "Motor & Spareparts", 65.00, "Astra Dili Battery Supply", 0.18),
            ("Wiper Kaca Mobil Silicon Pair", "Motor & Spareparts", 12.50, "Timor Auto Parts Lda", 0.30),
            ("Kaca Spion Mobil Universal", "Motor & Spareparts", 18.00, "Timor Auto Parts Lda", 0.25),
            ("Oli Mobil Shell Helix HX7 4L", "Motor & Spareparts", 32.00, "Castrol Timor Lda", 0.20),
            ("Sarung Jok Motor Anti Air", "Motor & Spareparts", 8.50, "Timor Safety Gear Lda", 0.35),
            ("Lampu Foglamp Halogen Mobil", "Motor & Spareparts", 15.00, "Timor Auto Parts Lda", 0.30),
            ("Car Charger USB & Phone Holder Mobil", "Motor & Spareparts", 9.50, "Timor Gadget Imports", 0.35),
        ]
    },
    {
        "name": "Dili Cosmetic & Beauty Store",
        "sector": "Kosmetik & Skincare",
        "location": "Colmera, Dili",
        "prefix": "KOS",
        "products": [
            ("Sunscreen SPF 50+ UV Shield 40ml", "Kosmetik & Beauty", 7.50, "Dili Beauty Supply Lda", 0.35),
            ("Facial Wash Brightening 100ml", "Kosmetik & Beauty", 4.25, "Dili Beauty Supply Lda", 0.32),
            ("Lipstik Matte Velvet Longlasting", "Kosmetik & Beauty", 6.50, "Dili Beauty Supply Lda", 0.40),
            ("Body Lotion Nivea Extra White 200ml", "Kosmetik & Beauty", 5.25, "Timor Consumer Imports", 0.30),
            ("Serum Wajah Niacinamide 20ml", "Kosmetik & Beauty", 9.50, "Dili Beauty Supply Lda", 0.38),
            ("Bedak Padat Two Way Cake Compact", "Kosmetik & Beauty", 8.00, "Dili Beauty Supply Lda", 0.35),
            ("Parfum Eau de Parfum 50ml", "Kosmetik & Beauty", 16.50, "Aroma Dili Trading Lda", 0.45),
            ("Micellar Water Cleanser 250ml", "Kosmetik & Beauty", 5.00, "Timor Consumer Imports", 0.30),
            ("Masker Wajah Sheet Mask (Pack)", "Kosmetik & Beauty", 2.25, "Dili Beauty Supply Lda", 0.40),
            ("Shampoo Herbal Anti-Dandruff 340ml", "Kosmetik & Beauty", 4.75, "Timor Consumer Imports", 0.28),
            ("Pensil Alis Waterproof Eyebrow", "Kosmetik & Beauty", 2.50, "Dili Beauty Supply Lda", 0.40),
            ("Krim Pelembab Wajah Moisturizer 50g", "Kosmetik & Beauty", 7.00, "Dili Beauty Supply Lda", 0.35),
            ("Sabun Mandi Cair Body Wash 450ml", "Kosmetik & Beauty", 3.80, "Timor Consumer Imports", 0.28),
        ]
    },
    {
        "name": "Timor Elektronik & Gadget",
        "sector": "Elektronik & Perangkat Rumah",
        "location": "Bebonuk, Dili",
        "prefix": "ELE",
        "products": [
            ("Kabel Data Fast Charging Type-C 1m", "Elektronik & Gadget", 3.50, "Timor Gadget Imports", 0.45),
            ("Powerbank 10.000mAh Dual Output", "Elektronik & Gadget", 14.50, "Timor Gadget Imports", 0.30),
            ("Earphone Bluetooth Wireless TWS", "Elektronik & Gadget", 12.00, "Timor Gadget Imports", 0.38),
            ("Kipas Angin Meja Desk Fan 10 Inch", "Elektronik & Gadget", 18.00, "Dili Electronics Supply", 0.25),
            ("Rice Cooker Mini 1.2 Liter", "Elektronik & Gadget", 24.50, "Dili Electronics Supply", 0.22),
            ("Setrika Listrik Dry Iron 350W", "Elektronik & Gadget", 15.00, "Dili Electronics Supply", 0.25),
            ("Lampu Bohlam LED Philips 12W", "Elektronik & Gadget", 4.00, "Dili Lighting Distribution", 0.30),
            ("Stop Kontak Colokan 4 Lubang + Kabel", "Elektronik & Gadget", 5.50, "Dili Electronics Supply", 0.35),
            ("Speaker Mini Bluetooth Portable", "Elektronik & Gadget", 11.50, "Timor Gadget Imports", 0.35),
            ("Charger HP Universal Fast Adapter", "Elektronik & Gadget", 6.00, "Timor Gadget Imports", 0.40),
            ("Kettle Teko Listrik Pemanas Air 1.8L", "Elektronik & Gadget", 13.50, "Dili Electronics Supply", 0.25),
            ("Antena TV Digital Indoor", "Elektronik & Gadget", 8.50, "Dili Electronics Supply", 0.35),
        ]
    }
]

def create_kafka_producer():
    """Membuat koneksi KafkaProducer dengan fallback aman."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            request_timeout_ms=3000,
            max_block_ms=3000,
        )
        return producer
    except Exception as e:
        print(f"[AVIZU] Servidor Kafka Broker standby ({e}). Dadus nafatin rai live ba MySQL.")
        return None

def get_mysql_connection():
    """Kria ligasaun ba MySQL sales_dashboard."""
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
            autocommit=True,
        )
        return conn
    except Exception as e:
        print(f"[ERRO] Labele liga ba MySQL: {e}")
        return None

def generate_transaction_data():
    """Membuat 1 transaksi acak dari 6 sektor toko di Dili dengan feature engineering."""
    store = random.choice(STORES_CATALOG)
    prod_tuple = random.choice(store["products"])
    
    prod_name, category, price, supplier, margin_rate = prod_tuple
    quantity = random.randint(1, 4)
    total_amount = round(price * quantity, 2)
    now = datetime.now()
    
    # Metode Pembayaran sesuai data lapangan di Dili:
    # - Toko Loilary & Lita Supermarket: Cash & QRIS (BRImo, Mosan, BNCTL)
    # - Dili Cosmetic & Beauty: Cash & QRIS (Mosan, BRImo)
    # - Bengkel/Dealer (Timor Motor), Elektronik, Fashion, ATK: cuma Cash
    s_name = store["name"]
    if "Loilary" in s_name or "Lita" in s_name:
        payment_method = random.choices(
            ["Cash", "QRIS (Mosan)", "QRIS (BRImo)", "QRIS (BNCTL)"],
            weights=[0.65, 0.15, 0.12, 0.08]
        )[0]
    elif "Cosmetic" in s_name or "Beauty" in s_name:
        payment_method = random.choices(
            ["Cash", "QRIS (Mosan)", "QRIS (BRImo)"],
            weights=[0.65, 0.20, 0.15]
        )[0]
    else:
        payment_method = "Cash"

    transaction_id = f"TRX-{store['prefix']}-{now.strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:4].upper()}"

    # Feature Engineering
    estimated_profit = round(total_amount * margin_rate, 2)
    is_high_value = 1 if total_amount >= 35.0 else 0
    is_peak_hour = 1 if now.hour in [11, 12, 13, 17, 18, 19] else 0
    bulk_tier = "Bulk" if quantity >= 4 else ("Medium" if quantity >= 2 else "Single")
    
    # JSON Payload untuk Kafka (Semi-Structured JSON)
    kafka_payload = {
        "transaction_id": transaction_id,
        "transaction_date": now.strftime("%Y-%m-%d %H:%M:%S"),
        "store": {
            "name": store["name"],
            "location": store["location"],
        },
        "product": {
            "name": prod_name,
            "category": category,
            "quantity": quantity,
            "price": price,
        },
        "payment": {
            "method": payment_method,
        },
        "total_amount": total_amount,
        "features": {
            "supplier": supplier,
            "margin_rate": margin_rate,
            "estimated_profit": estimated_profit,
            "is_high_value": is_high_value,
            "is_peak_hour": is_peak_hour,
            "bulk_tier": bulk_tier,
        }
    }
    
    # Tuple untuk MySQL Insert
    mysql_row = (
        transaction_id,
        store["name"],
        store["location"],
        now.strftime("%Y-%m-%d %H:%M:%S"),
        prod_name,
        category,
        quantity,
        price,
        total_amount,
        payment_method,
        supplier,
        margin_rate,
        estimated_profit,
        is_high_value,
        is_peak_hour,
        bulk_tier,
    )
    
    return kafka_payload, mysql_row, store["name"], prod_name, quantity, total_amount

def save_to_mysql(cursor, conn, mysql_row):
    """Menyimpan record langsung ke tabel transactions di MySQL dan sinkronisasi category_summary & payment_summary."""
    query = """
        INSERT IGNORE INTO transactions (
            transaction_id, store_name, store_location, transaction_date,
            product_name, category, quantity, price, total_amount,
            payment_method, supplier, margin_rate, estimated_profit,
            is_high_value, is_peak_hour, bulk_tier
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(query, mysql_row)

    # Sinkronisasi category_summary otomatis
    sync_query = """
        INSERT INTO category_summary (window_start, window_end, category, total_quantity, total_revenue)
        SELECT MIN(transaction_date), MAX(transaction_date), category, SUM(quantity), SUM(total_amount)
        FROM transactions
        WHERE category = %s
        GROUP BY category;
    """
    try:
        cursor.execute("DELETE FROM category_summary WHERE category = %s;", (mysql_row[5],))
        cursor.execute(sync_query, (mysql_row[5],))
    except Exception:
        pass

    # Sinkronisasi payment_summary otomatis
    pay_query = """
        INSERT INTO payment_summary (window_start, window_end, payment_method, total_transactions, total_revenue)
        SELECT MIN(transaction_date), MAX(transaction_date), payment_method, COUNT(*), SUM(total_amount)
        FROM transactions
        WHERE payment_method = %s
        GROUP BY payment_method;
    """
    try:
        cursor.execute("DELETE FROM payment_summary WHERE payment_method = %s;", (mysql_row[9],))
        cursor.execute(pay_query, (mysql_row[9],))
    except Exception:
        pass

def main():
    print("=" * 70)
    print("FLUXU DADUS REAL-TIME (KAFKA + MYSQL + POWER BI)")
    print("Setor Negosiu 6 iha Dili, Timor-Leste | Alvu Dadus: 12.531 Transasaun")
    print("=" * 70)
    print(f"-> Servidor Kafka : {KAFKA_BOOTSTRAP_SERVERS} | Topiku: {KAFKA_TOPIC}")
    print(f"-> Sink MySQL     : {MYSQL_HOST}:{MYSQL_PORT} | DB: {MYSQL_DATABASE}")
    print("-> Hanehan Ctrl + C atu para.")
    print("=" * 70)

    producer = create_kafka_producer()
    conn = get_mysql_connection()
    if not conn:
        print("[ERRO] Labele liga ba MySQL. Favór haree servisu MySQL Server.")
        sys.exit(1)

    cursor = conn.cursor()
    stream_count = 0

    try:
        while True:
            stream_count += 1
            kafka_payload, mysql_row, store_name, prod_name, qty, total = generate_transaction_data()
            tx_id = mysql_row[0]
            curr_time = datetime.now().strftime("%H:%M:%S")

            # 1. Kirim ke Kafka Broker
            kafka_status = "HEIN"
            if producer:
                try:
                    producer.send(KAFKA_TOPIC, value=kafka_payload)
                    producer.flush()
                    kafka_status = "HARUKA"
                except Exception:
                    kafka_status = "KOKO"

            # 2. Simpan ke MySQL (Tabel transactions)
            try:
                save_to_mysql(cursor, conn, mysql_row)
                mysql_status = "RAI ONA"
            except Exception as e:
                mysql_status = "ERRO"

            # 3. Tampilkan log di Terminal (Lian Tetun)
            print(
                f"[TRANSASAUN #{stream_count:03d}] {curr_time} | {tx_id} | "
                f"{store_name[:24]:<24} | {prod_name[:24]:<24} | "
                f"Qtd:{qty} | ${total:>6.2f} | [Kafka: {kafka_status} | MySQL: {mysql_status}]",
                flush=True
            )

            # Delay live realistis (1.0 - 2.0 detik)
            delay = random.uniform(PRODUCER_DELAY_MIN, PRODUCER_DELAY_MAX)
            time.sleep(delay)

    except KeyboardInterrupt:
        print("\n" + "=" * 70)
        print(f"[REMATADA] Fluxu dadus para ona hosi uzuariu.")
        print(f"Total transasaun foun ne'ebe haruka no rai ona: {stream_count} transasaun.")
        print("=" * 70)

    finally:
        if producer:
            producer.close()
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    main()