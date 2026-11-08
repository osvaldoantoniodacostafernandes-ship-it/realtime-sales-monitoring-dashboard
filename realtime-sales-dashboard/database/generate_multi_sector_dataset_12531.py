# ==============================================================================
# database/generate_multi_sector_dataset_12531.py
# Kria Dadus Komersial Multi-Setor iha Dili, Timor-Leste: TEPAT 12,531 Transasaun
# 6 Loja / Toko: Loilary (2024), Lita Supermarket (2023), Bengkel Motor (2023),
# Dealer Motor/Mobil (2020), Cosmetic (2024), Elektronik (2021)
# ==============================================================================

import os
import sys
import csv
import uuid
import random
from pathlib import Path
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import mysql.connector
import pandas as pd
from config.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)

OUTPUT_CSV_DIR = PROJECT_ROOT / "data"
OUTPUT_CSV_12531 = OUTPUT_CSV_DIR / "transactions_multi_store_12531.csv"
OUTPUT_CSV_13456 = OUTPUT_CSV_DIR / "transactions_multi_store_13456.csv"
OUTPUT_CSV_DEFAULT = OUTPUT_CSV_DIR / "transactions_multi_store_10k.csv"

# 6 Toko Komersial iha Dili ho Alvu Tepat 12,531 Dadus & Tinan Operasaun
STORES_SECTOR_CONFIG = [
    {
        "name": "Toko Loilary",
        "sector": "Supermarket & Sembako",
        "location": "Audian, Dili",
        "count": 2185,
        "prefix": "LOI",
        "start_year": 2024,
        "payment_rule": "qris_3_banks"  # Cash & QRIS (Mosan, BRImo, BNCTL)
    },
    {
        "name": "Lita Supermarket",
        "sector": "Supermarket Modern",
        "location": "Mandarin, Dili",
        "count": 2450,
        "prefix": "LIT",
        "start_year": 2023,
        "payment_rule": "qris_3_banks"  # Cash & QRIS (Mosan, BRImo, BNCTL)
    },
    {
        "name": "Bengkel Motor Dili",
        "sector": "Bengkel Servis Motor",
        "location": "Comoro, Dili",
        "count": 2064,
        "prefix": "BEN",
        "start_year": 2023,
        "payment_rule": "cash_only"  # 100% Cash
    },
    {
        "name": "Dealer Motor & Mobil Dili",
        "sector": "Dealer Kendaraan & Sparepart",
        "location": "Comoro, Dili",
        "count": 1842,
        "prefix": "DEA",
        "start_year": 2020,
        "payment_rule": "cash_only"  # 100% Cash
    },
    {
        "name": "Dili Cosmetic & Beauty Store",
        "sector": "Kosmetik & Skincare",
        "location": "Colmera, Dili",
        "count": 1970,
        "prefix": "KOS",
        "start_year": 2024,
        "payment_rule": "qris_2_banks"  # Cash & QRIS (Mosan, BRImo)
    },
    {
        "name": "Timor Elektronik & Gadget",
        "sector": "Elektronik & Perangkat Rumah",
        "location": "Bebonuk, Dili",
        "count": 2020,
        "prefix": "ELE",
        "start_year": 2021,
        "payment_rule": "cash_only"  # 100% Cash
    }
]

# Katalogu Produtu ba Kada Toko
CATALOG_BY_STORE = {
    "Toko Loilary": [
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
    ],
    "Lita Supermarket": [
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
    ],
    "Bengkel Motor Dili": [
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
    ],
    "Dealer Motor & Mobil Dili": [
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
    ],
    "Dili Cosmetic & Beauty Store": [
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
    ],
    "Timor Elektronik & Gadget": [
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


def generate_12531_dataset():
    print("=" * 75)
    print("KRIA DADUS REALISTIS MULTI-SETOR DILI: TEPAT 12.531 TRANSASAUN")
    print("Setor: Loilary (2024), Lita (2023), Bengkel (2023), Dealer (2020), Cosmetic (2024), Elektronik (2021)")
    print("=" * 75)

    total_target = sum(s["count"] for s in STORES_SECTOR_CONFIG)
    assert total_target == 12531, f"Alvu tenke 12,531 maibe hetan {total_target}"

    print(f"[INFO] Alvu: TEPAT 12.531 transasaun husi toko 6 iha Dili:")
    for s in STORES_SECTOR_CONFIG:
        print(f"  - {s['name']} (Setor: {s['sector']}, Tinan: {s['start_year']}-2026): {s['count']:,} dadus")

    random.seed(12531)
    end_date = datetime(2026, 10, 12, 18, 0, 0)
    all_transactions = []
    seq_counter = 1

    for store in STORES_SECTOR_CONFIG:
        s_name = store["name"]
        s_sec = store["sector"]
        s_loc = store["location"]
        s_pfx = store["prefix"]
        s_cnt = store["count"]
        s_yr = store["start_year"]
        p_rule = store["payment_rule"]
        catalog = CATALOG_BY_STORE[s_name]

        # Data hahu tuir tinan ne'ebe toko loke
        start_date = datetime(s_yr, 1, 15, 8, 0, 0)
        total_seconds_span = int((end_date - start_date).total_seconds())

        for _ in range(s_cnt):
            prod_info = random.choice(catalog)
            p_name, p_cat, p_price, p_supplier, p_margin = prod_info

            # Kuantidade kompra retallu 1-4
            qty = random.choices([1, 2, 3, 4], weights=[0.55, 0.30, 0.10, 0.05])[0]
            total_amount = round(p_price * qty, 2)

            # Metodu Pagamentu tuir peskiza kampu iha Dili:
            # - Loilary & Lita: Cash & QRIS (Mosan, BRImo, BNCTL)
            # - Cosmetic: Cash & QRIS (Mosan, BRImo)
            # - Bengkel, Dealer, Elektronik: 100% Cash
            if p_rule == "qris_3_banks":
                payment_method = random.choices(
                    ["Cash", "QRIS (Mosan)", "QRIS (BRImo)", "QRIS (BNCTL)"],
                    weights=[0.65, 0.15, 0.12, 0.08]
                )[0]
            elif p_rule == "qris_2_banks":
                payment_method = random.choices(
                    ["Cash", "QRIS (Mosan)", "QRIS (BRImo)"],
                    weights=[0.65, 0.20, 0.15]
                )[0]
            else:
                payment_method = "Cash"

            # Tempu transasaun (Distribuisaun husi tinan hahu to'o 2026)
            # Ponderasaun atu tinan foun liu hetan transasaun barak liu (realistiku)
            rand_ratio = random.random() ** 0.65  # Ponderasaun ba tempu resente
            offset_seconds = int(total_seconds_span * (1 - rand_ratio))
            raw_time = end_date - timedelta(seconds=offset_seconds)

            # Oras operasaun toko iha Dili (08:00 - 21:00)
            hour_choice = random.choices(
                population=list(range(8, 22)),
                weights=[2, 3, 4, 8, 9, 7, 5, 5, 8, 10, 9, 6, 3, 1]
            )[0]
            minute_choice = random.randint(0, 59)
            second_choice = random.randint(0, 59)

            txn_date = raw_time.replace(
                hour=hour_choice, minute=minute_choice, second=second_choice
            )

            txn_id = f"TRX-{s_pfx}-{txn_date.strftime('%Y%m%d')}-{seq_counter:05d}"
            seq_counter += 1

            est_profit = round(total_amount * p_margin, 2)
            is_high = 1 if total_amount >= 35.0 else 0
            is_peak = 1 if hour_choice in [11, 12, 13, 17, 18, 19] else 0
            bulk = "Bulk" if qty >= 4 else ("Medium" if qty >= 2 else "Single")

            all_transactions.append({
                "transaction_id": txn_id,
                "store_name": s_name,
                "store_location": s_loc,
                "transaction_date": txn_date.strftime("%Y-%m-%d %H:%M:%S"),
                "product_name": p_name,
                "category": p_cat,
                "quantity": qty,
                "price": p_price,
                "total_amount": total_amount,
                "payment_method": payment_method,
                "supplier": p_supplier,
                "margin_rate": p_margin,
                "estimated_profit": est_profit,
                "is_high_value": is_high,
                "is_peak_hour": is_peak,
                "bulk_tier": bulk
            })

    # Ordena tuir data kronolojiku
    all_transactions.sort(key=lambda x: x["transaction_date"])
    print(f"[OK] Total dadus kria ona: {len(all_transactions):,} transasaun.")

    # 1. Rai ba Ficheiru CSV
    OUTPUT_CSV_DIR.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "transaction_id", "store_name", "store_location", "transaction_date",
        "product_name", "category", "quantity", "price", "total_amount",
        "payment_method", "supplier", "margin_rate", "estimated_profit",
        "is_high_value", "is_peak_hour", "bulk_tier"
    ]

    for target_path in [OUTPUT_CSV_12531, OUTPUT_CSV_13456, OUTPUT_CSV_DEFAULT]:
        with open(target_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_transactions)
        print(f"[OK] Ficheiru CSV rai ona: {target_path.name}")

    # 2. Atualiza Baze Dadus MySQL (sales_dashboard)
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    cursor = conn.cursor()

    # Atualiza tabela master_dataset_target ba 12,531
    cursor.execute("""
        REPLACE INTO master_dataset_target (id, target_name, total_dataset_records, total_stores, description)
        VALUES (1, '6 Toko Komersial Dili (12,531 Data)', 12531, 6,
                '12,531 Transasaun Komersial Multi-Setor iha Dili (Loilary, Lita, Bengkel, Dealer, Kosmetika, Eletronika)');
    """)
    conn.commit()
    print("[OK] Tabela 'master_dataset_target' atualiza ba 12.531 dadus.")

    # 3. Garante tabela live 'transactions' mamuk (0 dadus) atu pronto ba streaming live
    print("[INFO] Hamoos tabela transasaun live (0 dadus) atu demonstra live husi zero...")
    cursor.execute("TRUNCATE TABLE transactions;")
    cursor.execute("TRUNCATE TABLE category_summary;")
    cursor.execute("TRUNCATE TABLE payment_summary;")
    conn.commit()
    print("[OK] Tabela live 'transactions' mamuk ona (0 dadus). Prontu ba demonstrasaun streaming!")

    cursor.close()
    conn.close()

    # 3. Kria Ficheiru Excel Kompletu ba Desktop (Bukti_Dataset_Excel_6_Toko_Dili)
    generate_excel_proof_desktop(all_transactions)

    print("=" * 75)
    print("SUCESU: DADUS 12.531 MULTI-SETOR PRONTU ONA HO LIAN TETUN!")
    print("=" * 75)


def generate_excel_proof_desktop(all_transactions):
    desktop_excel_dir = Path(r"C:\Users\LENOVO\Desktop\Bukti_Dataset_Excel_6_Toko_Dili")
    desktop_excel_dir.mkdir(parents=True, exist_ok=True)

    df_all = pd.DataFrame(all_transactions)

    # Hamoos ficheiru tuan se iha
    old_files = [
        "00_Ringkasan_Dataset_13456_Multi_Sektor.xlsx",
        "05_Centro_Moda_and_Fashion.xlsx",
        "06_Dili_Stationery_and_Office.xlsx",
        "01_Timor_Motor_and_Spareparts.xlsx",
        "04_Loilary_and_Lita_Supermarket.xlsx"
    ]
    for old in old_files:
        old_p = desktop_excel_dir / old
        if old_p.exists():
            try:
                old_p.unlink()
                print(f"[INFO] Hamoos ficheiru tuan: {old}")
            except Exception:
                pass

    excel_mapping = [
        ("01_Toko_Loilary.xlsx", "Toko Loilary", "2024 - 2026", "Cash & QRIS (Mosan, BRImo, BNCTL)"),
        ("02_Lita_Supermarket.xlsx", "Lita Supermarket", "2023 - 2026", "Cash & QRIS (Mosan, BRImo, BNCTL)"),
        ("03_Bengkel_Motor_Dili.xlsx", "Bengkel Motor Dili", "2023 - 2026", "100% Cash"),
        ("04_Dealer_Motor_dan_Mobil.xlsx", "Dealer Motor & Mobil Dili", "2020 - 2026", "100% Cash"),
        ("05_Dili_Cosmetic_and_Beauty.xlsx", "Dili Cosmetic & Beauty Store", "2024 - 2026", "Cash & QRIS (Mosan, BRImo)"),
        ("06_Timor_Elektronik_and_Gadget.xlsx", "Timor Elektronik & Gadget", "2021 - 2026", "100% Cash"),
    ]

    summary_rows = []

    for filename, store_name, years, pay_desc in excel_mapping:
        df_store = df_all[df_all["store_name"] == store_name].copy()
        file_path = desktop_excel_dir / filename

        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
            df_store.to_excel(writer, sheet_name="Transasaun", index=False)
        print(f"[OK] Ficheiru Excel loja rai ona: {filename} ({len(df_store):,} dadus)")

        summary_rows.append({
            "Naran Toko / Loja": store_name,
            "Setor Negosiu": df_store["category"].iloc[0] if len(df_store) > 0 else "-",
            "Fatin (Lokalizasaun)": df_store["store_location"].iloc[0] if len(df_store) > 0 else "Dili",
            "Tinan Operasaun": years,
            "Total Transasaun": len(df_store),
            "Total Reseita (USD)": round(df_store["total_amount"].sum(), 2),
            "Metodu Pagamentu": pay_desc,
        })

    # Kria Ficheiru Sumariu 00
    df_summary = pd.DataFrame(summary_rows)
    total_row = pd.DataFrame([{
        "Naran Toko / Loja": "TOTAL GERAL (6 TOKO DILI)",
        "Setor Negosiu": "Multi-Setor Komersial",
        "Fatin (Lokalizasaun)": "Dili, Timor-Leste",
        "Tinan Operasaun": "2020 - 2026",
        "Total Transasaun": len(df_all),
        "Total Reseita (USD)": round(df_all["total_amount"].sum(), 2),
        "Metodu Pagamentu": "Cash & QRIS (Mosan, BRImo, BNCTL)",
    }])
    df_summary_final = pd.concat([df_summary, total_row], ignore_index=True)

    summary_file = desktop_excel_dir / "00_Ringkasan_Dataset_12531_Multi_Sektor.xlsx"
    with pd.ExcelWriter(summary_file, engine="openpyxl") as writer:
        df_summary_final.to_excel(writer, sheet_name="Sumariu 6 Toko", index=False)
        df_all.to_excel(writer, sheet_name="Dadus Kompletu 12531", index=False)

    print(f"[OK] Ficheiru Sumariu Excel rai ona: {summary_file.name} (12.531 dadus kompletu)")


if __name__ == "__main__":
    generate_12531_dataset()
