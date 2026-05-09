
"""
consumer.py
-----------
Consumes order messages from Kafka, batches them, and writes one CSV file
to S3 partitioned by date:

    s3://<bucket>/stream_store/year=YYYY/month=MM/day=DD/batch_<ts>_<id>.csv

Exits cleanly after CONSUMER_IDLE_TIMEOUT_S seconds of no new messages
so Airflow marks the task SUCCESS.

Airflow Connection required:
    aws_default  →  Login = AWS Access Key ID
                    Password = AWS Secret Access Key
                    Extra (JSON) = {"region_name": "us-east-1", "s3_bucket": "your-bucket-name"}
"""

import csv
import io
import json
import logging
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaException

from airflow.hooks.base import BaseHook

log = logging.getLogger(__name__)

# ── Hardcoded settings ─────────────────────────────────────────────────────────
KAFKA_TOPIC              = "stream_store_topic" 
KAFKA_BOOTSTRAP_SERVERS  = "localhost:9092"   # override if broker is remote
KAFKA_GROUP_ID           = "stream_store-s3-consumer-group"
S3_PREFIX                = "streamstore"
BATCH_SIZE               = 8     # flush to S3 every 20 rows
BATCH_TIMEOUT_S          = 30     # flush after 60s even if batch isn't full
CONSUMER_IDLE_TIMEOUT_S  = 30     # exit after 30s of no new messages

bucket_name = "streamstore-081653452945-ap-south-1-an"

# ── S3 helpers ─────────────────────────────────────────────────────────────────
def _build_s3_client():
    """
    Builds boto3 S3 client from the 'aws_default' Airflow Connection.
    s3_bucket, region, and credentials all come from that connection.
    """
    conn  = BaseHook.get_connection("aws_default")
    extra = json.loads(conn.extra) if conn.extra else {}

    kwargs = {"region_name": extra.get("region_name", "us-east-1")}
    if conn.login:                           # explicit keys → use them
        kwargs["aws_access_key_id"]     = conn.login
        kwargs["aws_secret_access_key"] = conn.password
    # else: boto3 falls back to IAM role / env vars automatically

    bucket = extra.get("s3_bucket", "")
    if not bucket:
        raise ValueError(
            "s3_bucket is missing from aws_default connection Extra JSON. "
            "Add: {\"region_name\": \"us-east-1\", \"s3_bucket\": \"your-bucket\"}"
        )
    return boto3.client("s3", **kwargs), bucket

def _build_s3_key(ts: datetime, batch_id: int) -> str:
    return (
        f"{S3_PREFIX}/"
        f"year_{ts.strftime('%Y')}_"
        f"month_{ts.strftime('%m')}_"
        f"day_{ts.strftime('%d')}_"
        f"batch_{ts.strftime('%Y%m%d_%H%M%S')}_{batch_id:04d}.csv"
    )


def _batch_to_csv_bytes(batch: list[dict]) -> bytes:
    buf    = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=list(batch[0].keys()), lineterminator="\n")
    writer.writeheader()
    writer.writerows(batch)
    return buf.getvalue().encode("utf-8")


def _flush_to_s3(s3_client, bucket: str, batch: list[dict], batch_id: int) -> str | None:
    if not batch:
        return None
    ts        = datetime.now(tz=timezone.utc)
    key       = _build_s3_key(ts, batch_id)
    csv_bytes = _batch_to_csv_bytes(batch)
    try:
        s3_client.put_object(Bucket=bucket, Key=key, Body=csv_bytes, ContentType="text/csv")
        log.info("✔ Batch #%04d → s3://%s/%s  (%d rows, %.1f KB)",
                 batch_id, bucket, key, len(batch), len(csv_bytes) / 1024)
        return key
    except ClientError as exc:
        log.error("S3 upload failed for batch #%04d: %s", batch_id, exc)
        return None


# ── Main callable ──────────────────────────────────────────────────────────────
def consume_data(**context) -> dict:
    """
    Airflow PythonOperator callable.
    Drains the Kafka topic, writes batched CSVs to S3, exits on idle.
    """
    s3_client, bucket = _build_s3_client()

    # consumer = Consumer(
    #     KAFKA_TOPIC,
    #     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    #     group_id=KAFKA_GROUP_ID,
    #     auto_offset_reset="earliest",
    #     enable_auto_commit=False,        # commit only after successful S3 write
    #     value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    #     max_poll_interval_ms=300_000,
    #     session_timeout_ms=30_000,
    #     heartbeat_interval_ms=10_000,
    # )

    consumer = Consumer({
        "bootstrap.servers": "kafka:9092",
        "group.id": "stream_store-s3-consumer-group",
        "auto.offset.reset": "earliest"
    })

    consumer.subscribe(["stream_store_topic"])

    log.info(
        "Consumer started. batch_size=%d  batch_timeout=%ds  idle_exit=%ds",
        BATCH_SIZE, BATCH_TIMEOUT_S, CONSUMER_IDLE_TIMEOUT_S,
    )

    batch: list[dict] = []
    batch_id          = 0
    total_rows        = 0
    total_files       = 0
    last_flush_time   = time.monotonic()
    last_message_time = time.monotonic()
    retry_count = 0

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                pass
            elif msg.error():
                log.error(msg.error())
            else:
                batch.append(json.loads(msg.value().decode("utf-8")))
                last_message_time = time.monotonic()

            # ── Idle exit ──────────────────────────────────────────────────────
            if time.monotonic() - last_message_time >= CONSUMER_IDLE_TIMEOUT_S:
                log.info("Idle for %ds — exiting.", CONSUMER_IDLE_TIMEOUT_S)
                break

            # ── Flush decision ─────────────────────────────────────────────────
            size_trigger = len(batch) >= BATCH_SIZE
            time_trigger = batch and (time.monotonic() - last_flush_time >= BATCH_TIMEOUT_S)

            if size_trigger or time_trigger:
                reason = "size" if size_trigger else "timeout"
                log.info("Flushing batch #%04d (%d rows) — %s", batch_id, len(batch), reason)
                key = _flush_to_s3(s3_client, bucket, batch, batch_id)
                if key:
                    consumer.commit()       # safe to commit — data is in S3
                    total_rows  += len(batch)
                    total_files += 1
                    batch_id    += 1
                    batch        = []
                    last_flush_time = time.monotonic()
                    retry_count = 0
                else:
                    log.warning("Retaining batch — will retry on next flush.")
                    retry_count += 1
            
            
            if retry_count > 2:
                log.error("Dropping batch after repeated S3 failures")
                batch = []
                retry_count = 0
        
        
        # ── Final flush ────────────────────────────────────────────────────────
        if batch:
            log.info("Final flush: %d rows.", len(batch))
            key = _flush_to_s3(s3_client, bucket, batch, batch_id)
            if key:
                consumer.commit()
                total_rows  += len(batch)
                total_files += 1

    except KafkaException as exc:
        log.error("Kafka error: %s", exc)
        raise
    finally:
        consumer.close()
        log.info("Consumer done. rows=%d  files=%d", total_rows, total_files)

    return {"total_rows": total_rows, "total_files": total_files}
