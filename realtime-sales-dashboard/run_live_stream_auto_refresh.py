# ==============================================================================
# run_live_stream_auto_refresh.py
# Pipeline Fluxu Dadus Real-Time & Atualizasaun Automatika ba Power BI
# Capstone Big Data DIT 2026 - Painel Vendas Retallu Multi-Setor Dili
# ==============================================================================

import os
import sys
import time
import threading
import subprocess
from pathlib import Path
from datetime import datetime

# Garante kompatibilidade UTF-8 iha Windows Terminal
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.append(str(PROJECT_ROOT))

# Import lojika streaming hosi producer
from producer.kafka_producer import (
    create_kafka_producer,
    get_mysql_connection,
    generate_transaction_data,
    save_to_mysql,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_DATABASE,
)

# Intervalu atualizasaun automatika ba Power BI (segundu)
PBI_REFRESH_INTERVAL_SECONDS = 4

# Sinais atu hapara background thread
stop_flag = threading.Event()


def trigger_powerbi_refresh():
    """
    Haruka sinais atualizasaun (Ctrl + R / F5) ba janela Power BI Desktop.
    Uza WScript.Shell iha Windows Session.
    """
    ps_cmd = """
    $ws = New-Object -ComObject WScript.Shell
    if ($ws.AppActivate("Power BI Desktop")) {
        Start-Sleep -Milliseconds 100
        $ws.SendKeys("^r")
        Start-Sleep -Milliseconds 50
        $ws.SendKeys("{F5}")
    } elseif ($ws.AppActivate("Power BI")) {
        Start-Sleep -Milliseconds 100
        $ws.SendKeys("^r")
        Start-Sleep -Milliseconds 50
        $ws.SendKeys("{F5}")
    }
    """
    try:
        subprocess.Popen(
            ["powershell", "-NoProfile", "-WindowStyle", "Hidden", "-Command", ps_cmd],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    except Exception:
        pass


def auto_refresh_worker():
    """Worker iha kotuk ne'ebe haruka atualizasaun ba Power BI beibeik."""
    time.sleep(2.5)
    while not stop_flag.is_set():
        trigger_powerbi_refresh()
        for _ in range(int(PBI_REFRESH_INTERVAL_SECONDS * 10)):
            if stop_flag.is_set():
                break
            time.sleep(0.1)


def main():
    print("=" * 75)
    print("FLUXU DADUS REAL-TIME & ATUALIZASAUN POWER BI (DILI MULTI-SETOR)")
    print("Painel Vendas Retallu Dili (Setor Negosiu 6 | Alvu: Dadus 12.531)")
    print("=" * 75)
    print(f"-> Servidor Kafka   : {KAFKA_BOOTSTRAP_SERVERS} | Topiku: {KAFKA_TOPIC}")
    print(f"-> Baze Dadus MySQL : {MYSQL_HOST}:{MYSQL_PORT} | DB: {MYSQL_DATABASE}")
    print(f"-> Atualizasaun PBI : Automatiku kada sekundu {PBI_REFRESH_INTERVAL_SECONDS}")
    print("-> Atu Para         : Hanehan Ctrl + C")
    print("=" * 75)

    producer = create_kafka_producer()
    conn = get_mysql_connection()
    if not conn:
        print("[ERRO] Labele liga ba MySQL. Favor haree servisu MySQL Server.")
        sys.exit(1)

    cursor = conn.cursor()
    stream_count = 0

    # Loke background thread ba atualizasaun Power BI
    refresh_thread = threading.Thread(target=auto_refresh_worker, daemon=True)
    refresh_thread.start()

    try:
        while True:
            stream_count += 1
            kafka_payload, mysql_row, store_name, prod_name, qty, total = generate_transaction_data()
            tx_id = mysql_row[0]
            curr_time = datetime.now().strftime("%H:%M:%S")

            # 1. Haruka ba Kafka
            kafka_status = "HEIN"
            if producer:
                try:
                    producer.send(KAFKA_TOPIC, value=kafka_payload)
                    producer.flush()
                    kafka_status = "HARUKA"
                except Exception:
                    kafka_status = "KOKO"

            # 2. Rai ba MySQL
            try:
                save_to_mysql(cursor, conn, mysql_row)
                mysql_status = "RAI ONA"
            except Exception as e:
                mysql_status = "ERRO"

            # 3. Hatudu iha Terminal ho lian Tetun
            print(
                f"[TRANSASAUN #{stream_count:03d}] {curr_time} | {tx_id} | "
                f"{store_name[:22]:<22} | {prod_name[:22]:<22} | "
                f"Qtd:{qty} | ${total:>6.2f} | [Kafka: {kafka_status} | MySQL: {mysql_status}]",
                flush=True
            )

            # Tempu hein entre transasaun (1.5 - 2.5 segundu)
            time.sleep(1.8)

    except KeyboardInterrupt:
        stop_flag.set()
        print("\n" + "=" * 75)
        print("[REMATADA] Fluxu dadus para ona hosi uzuariu.")
        print(f"Total Transasaun Foun ne'ebe haruka no rai ona: {stream_count} transasaun.")
        print("Dadus hotu rai ona ho seguru iha baze dadus MySQL!")
        print("=" * 75)

    finally:
        stop_flag.set()
        if producer:
            producer.close()
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
