#!/usr/bin/env python3
"""
Kafka consumer: Reads real-time weather observations and writes to PostgreSQL.

Consumes messages from the weather.observations.raw topic, applies basic
validation, and inserts into the raw.weather_stream table for downstream
processing.

Usage:
    python weather_consumer.py
"""

import json
import logging
import os

import psycopg2

from kafka import KafkaConsumer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "weather.observations.raw"
CONSUMER_GROUP = "weather-stream-consumer"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "weather_warehouse"),
    "user": os.getenv("DB_USER", "weather"),
    "password": os.getenv("DB_PASSWORD", "weather_pipeline_2026"),
}

VALID_ELEMENTS = {"TMAX", "TMIN", "PRCP", "SNOW", "SNWD", "AWND", "TAVG"}
BATCH_SIZE = 100

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)


def create_consumer():
    """Create and return a Kafka consumer."""
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )


def validate_message(msg: dict) -> bool:
    """Basic validation of incoming weather observation."""
    required_fields = {"station_id", "obs_date", "element", "data_value"}
    if not required_fields.issubset(msg.keys()):
        return False
    if msg["element"] not in VALID_ELEMENTS:
        return False
    if not isinstance(msg["data_value"], (int, float)):
        return False
    return True


def write_batch_to_postgres(batch: list[dict]):
    """Write a batch of validated messages to the stream table."""
    if not batch:
        return

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    insert_sql = """
        INSERT INTO raw.weather_stream (station_id, obs_date, element, data_value)
        VALUES (%s, %s, %s, %s)
    """

    records = [(msg["station_id"], msg["obs_date"], msg["element"], msg["data_value"]) for msg in batch]

    cur.executemany(insert_sql, records)
    conn.commit()
    cur.close()
    conn.close()

    logger.info("Wrote batch of %d records to raw.weather_stream", len(batch))


def consume():
    """Main consumer loop: read, validate, batch-write."""
    consumer = create_consumer()
    logger.info("Consumer started. Listening on topic: %s", TOPIC)

    batch = []
    total_consumed = 0
    total_invalid = 0

    try:
        for message in consumer:
            msg = message.value

            if validate_message(msg):
                batch.append(msg)
            else:
                total_invalid += 1
                logger.debug("Invalid message skipped: %s", msg)

            if len(batch) >= BATCH_SIZE:
                write_batch_to_postgres(batch)
                total_consumed += len(batch)
                batch = []
                logger.info(
                    "Progress: %d consumed, %d invalid",
                    total_consumed,
                    total_invalid,
                )

    except KeyboardInterrupt:
        logger.info("Consumer interrupted. Flushing remaining batch …")
        if batch:
            write_batch_to_postgres(batch)
            total_consumed += len(batch)

    finally:
        consumer.close()
        logger.info(
            "Consumer stopped. Total: %d consumed, %d invalid.",
            total_consumed,
            total_invalid,
        )


if __name__ == "__main__":
    consume()
