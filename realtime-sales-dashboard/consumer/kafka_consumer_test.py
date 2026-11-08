# =========================================================
# kafka_consumer_test.py
# Script ini HANYA untuk testing/debugging -- untuk memastikan
# data yang dikirim producer benar-benar sampai di Kafka,
# SEBELUM kita pakai Spark yang lebih rumit.
#
# Cara jalankan (di terminal terpisah, sambil producer jalan):
#   python kafka_consumer_test.py
#
# Hentikan dengan Ctrl + C
# =========================================================

import json
import sys

from kafka import KafkaConsumer

sys.path.append("../config")
from config.config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="latest",   # hanya baca pesan baru setelah consumer jalan
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print(f"[INFO] Mendengarkan topic: '{KAFKA_TOPIC}'")
    print("[INFO] Tekan Ctrl+C untuk berhenti.\n")

    try:
        for message in consumer:
            data = message.value
            print(f"[RECEIVED] {data}")
    except KeyboardInterrupt:
        print("\n[INFO] Consumer dihentikan oleh user.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
