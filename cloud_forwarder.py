"""
Store-and-Forward Cloud Sync Service
Batches unsynced local SQLite telemetry records and dispatches them to cloud infrastructure.
"""

import json
import logging
import os
import sqlite3
import threading
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("CloudForwarder")

DB_PATH = os.path.join("data", "telemetry.db")
CLOUD_TELEMETRY_TOPIC = "cloud/enterprise/beverage/telemetry_ingest"
BATCH_SIZE = 25


class CloudForwarder:
    def __init__(self):
        self.connected_event = threading.Event()
        self.client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id="Edge_Cloud_Forwarder",
            clean_session=True
        )
        self.client.on_connect = self.on_connect

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("Forwarder connected to broker.")
            self.connected_event.set()
        else:
            logger.error("Connection failed with code: %s", reason_code)

    def get_unsynced_batch(self, limit=BATCH_SIZE):
        """Fetches pending unsynced records from the local SQLite store."""
        if not os.path.exists(DB_PATH):
            return []

        with sqlite3.connect(DB_PATH) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, asset_id, timestamp, temperature_pv, flow_rate_pv, 
                       tank_level_pv, temperature_sp, flow_rate_sp, 
                       heater_duty_percent, operational_state
                FROM process_telemetry
                WHERE synced_to_cloud = 0
                ORDER BY id ASC
                LIMIT ?
            """, (limit,))
            rows = cursor.fetchall()
            return [dict(row) for row in rows]

    def mark_records_synced(self, record_ids):
        """Updates synchronization status after successful transmission."""
        if not record_ids:
            return

        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            placeholders = ",".join("?" for _ in record_ids)
            cursor.execute(f"""
                UPDATE process_telemetry
                SET synced_to_cloud = 1
                WHERE id IN ({placeholders})
            """, record_ids)
            conn.commit()
        logger.info("Marked %d records as synced to cloud.", len(record_ids))

    def run_sync_cycle(self):
        self.client.connect(config.BROKER_HOST, config.BROKER_PORT, config.KEEPALIVE_INTERVAL)
        self.client.loop_start()

        logger.info("Waiting for broker connection confirmation...")
        if not self.connected_event.wait(timeout=10.0):
            logger.error("Connection timed out. Aborting sync.")
            self.client.loop_stop()
            return

        logger.info("Starting edge-to-cloud batch forwarder cycle...")

        records = self.get_unsynced_batch(BATCH_SIZE)
        if not records:
            logger.info("Local store is fully synchronized. Zero pending records.")
            self.client.loop_stop()
            self.client.disconnect()
            return

        logger.info("Found %d pending records. Packaging into cloud ingestion payload...", len(records))

        payload = {
            "ingest_timestamp": datetime.now(timezone.utc).isoformat(),
            "batch_count": len(records),
            "source_facility": config.FACILITY,
            "source_line": config.LINE,
            "records": records
        }

        # Dispatch batch with QoS 1 to guarantee delivery
        info = self.client.publish(CLOUD_TELEMETRY_TOPIC, json.dumps(payload), qos=1)
        info.wait_for_publish()

        record_ids = [r["id"] for r in records]
        self.mark_records_synced(record_ids)

        logger.info("Successfully dispatched batch to %s", CLOUD_TELEMETRY_TOPIC)
        self.client.loop_stop()
        self.client.disconnect()


if __name__ == "__main__":
    forwarder = CloudForwarder()
    forwarder.run_sync_cycle()