# ==============================================================================
# database/reset_live_transactions.py
# Reset / Truncate table 'transactions' back to 0
# Berguna untuk demonstrasi live streaming dari 0 di hadapan dosen
# ==============================================================================

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import mysql.connector
from config.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)

def reset_transactions():
    print("=" * 60)
    print("HAMOOS DADUS TRANSASAUN LIVE (RESET BA 0)")
    print("=" * 60)

    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
        )
        cursor = conn.cursor()

        # Truncate / hamoos transasaun live
        cursor.execute("TRUNCATE TABLE transactions;")
        
        # Hamoos tabela sumariu se iha
        for tbl in ["sales_summary", "category_summary", "payment_summary", "store_summary"]:
            try:
                cursor.execute(f"TRUNCATE TABLE {tbl};")
            except Exception:
                pass

        conn.commit()
        cursor.execute("SELECT COUNT(*) FROM transactions;")
        count = cursor.fetchone()[0]

        cursor.execute("SELECT total_dataset_records FROM master_dataset_target LIMIT 1;")
        target_row = cursor.fetchone()
        target_count = target_row[0] if target_row else 12531

        cursor.close()
        conn.close()

        print(f"[SUSESU] Tabela 'transactions' mamuk ona!")
        print(f"-> Dadus Transasaun Live  : {count} dadus")
        print(f"-> Alvu Big Data Dataset : {target_count:,} dadus")
        print("=" * 60)
        print("Baze dadus prontu ona atu simu fluxu foun!")
        print("Loke terminal: python run_live_stream_auto_refresh.py")
        print("=" * 60)

    except Exception as e:
        print(f"[ERRO] Labele hamoos baze dadus: {e}")

if __name__ == "__main__":
    reset_transactions()
