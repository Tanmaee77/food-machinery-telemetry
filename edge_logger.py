"""
Edge SQLite Logger
Subscribes to machine telemetry and persists records locally (Store-and-Forward pattern).
"""

import json
import logging
import os
import sqlite3
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("EdgeLogger")

DB_DIR = "data"
DB_PATH = os.path.join(DB_DIR, "telemetry.db")


class EdgeTelemetryLogger:
    def __init__(self):
        self._init_database()

        self.client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id="EdgeSQLiteLogger",
            clean_session=False  # Persistent session to prevent missed telemetry
        )

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def _init_database(self):
        """Ensures the local SQLite schema is provisioned."""
        os.makedirs(DB_DIR, exist_ok=True)
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS process_telemetry (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    asset_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    temperature_pv REAL,
                    flow_rate_pv REAL,
                    tank_level_pv REAL,
                    temperature_sp REAL,
                    flow_rate_sp REAL,
                    heater_duty_percent REAL,
                    operational_state TEXT,
                    synced_to_cloud INTEGER DEFAULT 0
                )
            """)
            conn.commit()
        logger.info("Database schema initialized at %s", DB_PATH)

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("Logger connected to MQTT broker.")
            # Subscribe to the edge telemetry feed
            self.client.subscribe(config.TOPIC_TELEMETRY, qos=1)
            logger.info("Subscribed to telemetry stream: %s", config.TOPIC_TELEMETRY)
        else:
            logger.error("Connection failed with code: %s", reason_code)

    def on_message(self, client, userdata, msg):
        """Unpacks incoming telemetry payload and writes to SQLite."""
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            
            asset_id = payload.get("asset_id")
            timestamp = payload.get("timestamp")
            pvs = payload.get("process_values", {})
            sps = payload.get("setpoints", {})
            acts = payload.get("actuators", {})
            state = payload.get("operational_state", "UNKNOWN")

            with sqlite3.connect(DB_PATH) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO process_telemetry (
                        asset_id, timestamp, temperature_pv, flow_rate_pv, 
                        tank_level_pv, temperature_sp, flow_rate_sp, 
                        heater_duty_percent, operational_state, synced_to_cloud
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """, (
                    asset_id,
                    timestamp,
                    pvs.get("temperature_pv"),
                    pvs.get("flow_rate_pv"),
                    pvs.get("tank_level_pv"),
                    sps.get("temperature_sp"),
                    sps.get("flow_rate_sp"),
                    acts.get("heater_duty_percent"),
                    state
                ))
                conn.commit()

            logger.info("Logged record | Temp: %s C | Flow: %s LPM | State: %s",
                        pvs.get("temperature_pv"), pvs.get("flow_rate_pv"), state)

        except json.JSONDecodeError:
            logger.error("Unable to decode packet payload: %s", msg.payload)
        except sqlite3.Error as e:
            logger.error("Database write failure: %s", e)

    def start(self):
        self.client.connect(config.BROKER_HOST, config.BROKER_PORT, config.KEEPALIVE_INTERVAL)
        logger.info("Starting edge logger event loop...")
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            logger.info("Stopping edge logger...")
            self.client.disconnect()


if __name__ == "__main__":
    logger_service = EdgeTelemetryLogger()
    logger_service.start()