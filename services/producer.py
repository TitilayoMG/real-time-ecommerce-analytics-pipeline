
"""
producer.py
-----------
Reads stream_store_dataset.csv row-by-row using an offset stored in an Airflow Variable
so each DAG run streams the next 20 rows only — simulating real-time ingestion.

Airflow Variable required:
    csv_offset  →  starts at 0, auto-incremented by 8 after each run

Airflow Connection required:
    kafka_default  →  Host = broker host, Port = 9092
"""

import csv
import json
import logging
import time

from airflow.hooks.base import BaseHook
from airflow.models import Variable
from confluent_kafka import Producer, KafkaException

log = logging.getLogger(__name__)

# ── Hardcoded settings ─────────────────────────────────────────────────────────
KAFKA_TOPIC     = "stream_store_topic"
# ORDER_CSV_PATH  = "/mnt/c/Users/DELL/Documents/de_dataset/order.csv"  # "/opt/airflow/data/stream_store_dataset.csv"
ORDER_CSV_PATH = "/opt/airflow/data/stream_dataset.csv"
ROWS_PER_RUN    = 8       # rows to stream per DAG run
PRODUCE_DELAY_S = 0.0      # set > 0 only for local testing

def delivery_report(err, msg):
    if err is not None:
        log.error(f"Delivery failed: {err}")
    else:
        log.info(f"Delivered to {msg.topic()} [{msg.partition()}]")


def produce_data(**context) -> dict:
    """
    Airflow PythonOperator callable.
    Sends the next ROWS_PER_RUN rows to Kafka based on csv_offset Variable.
    Updates the offset after successful send.
    """

    # ── Read and advance offset ────────────────────────────────────────────────
    offset = int(Variable.get("csv_offset", default_var="0"))
    log.info("Producer starting at offset %d — will send %d rows", offset, ROWS_PER_RUN)


    producer_config = {
        "bootstrap.servers":  "kafka:9092"
    }

    producer = Producer(producer_config)

    sent = 0

    try:
        with open(ORDER_CSV_PATH, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for i, row in enumerate(reader):

                if i < offset:          # skip rows already processed
                    continue
                if sent >= ROWS_PER_RUN:  # stop after batch limit
                    break

                # future = producer.send(KAFKA_TOPIC, value=row)
                # future.get(timeout=10)  # block so errors surface immediately
                producer.produce(
                    KAFKA_TOPIC,
                    value=json.dumps(row).encode("utf-8"),
                    callback=delivery_report
                )
                producer.poll(0)

                sent += 1

                if PRODUCE_DELAY_S > 0:
                    time.sleep(PRODUCE_DELAY_S)

        producer.flush(20)

    except FileNotFoundError:
        log.error("CSV not found: %s", ORDER_CSV_PATH)
        raise
    except KafkaException as exc:
        log.error("Kafka send error: %s", exc)
        raise

    # ── Advance offset for next run ────────────────────────────────────────────
    new_offset = offset + sent
    Variable.set("csv_offset", new_offset)
    log.info("Producer done. Rows sent: %d  |  Next offset: %d", sent, new_offset)

    return {"rows_sent": sent, "next_offset": new_offset}