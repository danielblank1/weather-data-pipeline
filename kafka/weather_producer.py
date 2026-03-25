#!/usr/bin/env python3
"""
Kafka producer: Simulates real-time weather data streaming.

Reads historical GHCN data from PostgreSQL and replays it through Kafka
at an accelerated rate, simulating a live weather data feed.

This is useful for demonstrating streaming capabilities and for testing
the consumer/processing pipeline.

Usage:
    python weather_producer.py
    python weather_producer.py --station USW00094728 --speed 100
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime

import psycopg2
from kafka import KafkaProducer

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "weather.observations.raw"

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.getenv("DB_NAME", "weather_warehouse"),
    "user": os.getenv("DB_USER", "weather"),
    "password": os.getenv("DB_PASSWORD", "weather_pipeline_2026"),
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)


def create_producer():
    """Create and return a Kafka producer."""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=3,
    )


def fetch_observations(station_id=None, limit=10000):
    """Fetch recent observations from PostgreSQL to replay."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    query = """
        SELECT station_id, obs_date, element, data_value
        FROM raw.ghcn_daily
        WHERE data_value IS NOT NULL
    """
    params = []

    if station_id:
        query += " AND station_id = %s"
        params.append(station_id)

    query += " ORDER BY obs_date DESC LIMIT %s"
    params.append(limit)

    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return rows


def produce_messages(producer, rows, speed_factor=10):
    """
    Send observations to Kafka, simulating real-time data arrival.

    Args:
        speed_factor: How many times faster than real-time to replay.
                      100 = 1 day of data replayed in ~14 minutes.
    """
    logger.info(
        "Starting producer: %d messages, speed=%dx, topic=%s",
        len(rows), speed_factor, TOPIC,
    )

    sent = 0
    for station_id, obs_date, element, value in rows:
        message = {
            "station_id": station_id,
            "obs_date": obs_date.isoformat() if hasattr(obs_date, "isoformat") else str(obs_date),
            "element": element,
            "data_value": value,
            "produced_at": datetime.utcnow().isoformat(),
        }

        producer.send(
            TOPIC,
            key=station_id,
            value=message,
        )
        sent += 1

        if sent % 1000 == 0:
            logger.info("Sent %d / %d messages", sent, len(rows))
            producer.flush()

        # Simulate inter-arrival delay
        time.sleep(1.0 / speed_factor)

    producer.flush()
    logger.info("Producer finished: %d messages sent.", sent)


def main():
    parser = argparse.ArgumentParser(description="Weather Kafka producer")
    parser.add_argument("--station", default=None, help="Filter to station ID")
    parser.add_argument("--speed", type=int, default=100, help="Replay speed factor")
    parser.add_argument("--limit", type=int, default=10000, help="Max messages")
    args = parser.parse_args()

    producer = create_producer()

    rows = fetch_observations(station_id=args.station, limit=args.limit)
    if not rows:
        logger.error("No data found. Run the ingestion pipeline first.")
        return

    produce_messages(producer, rows, speed_factor=args.speed)
    producer.close()


if __name__ == "__main__":
    main()
