"""
Remote Supervisory Command Dispatcher
Synchronized connection handling with automated recipe sequence.
"""

import json
import logging
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
logger = logging.getLogger("CommandDispatcher")


class RemoteCommandDispatcher:
    def __init__(self):
        self.connected_event = threading.Event()
        self.client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id="Supervisory_Dispatcher",
            clean_session=True
        )

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("Dispatcher connected to broker.")
            self.client.subscribe(config.TOPIC_COMMAND_ACK, qos=1)
            logger.info("Subscribed to ACK channel: %s", config.TOPIC_COMMAND_ACK)
            self.connected_event.set()
        else:
            logger.error("Connection failed with code: %s", reason_code)

    def on_message(self, client, userdata, msg):
        try:
            ack_data = json.loads(msg.payload.decode("utf-8"))
            status = "SUCCESS" if ack_data.get("success") else "REJECTED"
            logger.info("[%s] Command ID: %s | Message: %s | State: %s",
                        status,
                        ack_data.get("command_id"),
                        ack_data.get("status_message"),
                        ack_data.get("applied_state"))
        except json.JSONDecodeError:
            logger.error("Received unparseable ACK: %s", msg.payload)

    def send_command(self, cmd_id: str, command: str, value):
        command_packet = {
            "command_id": cmd_id,
            "target_asset": config.ASSET,
            "command": command,
            "value": value,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        logger.info("Issuing command [%s]: %s -> %s", cmd_id, command, value)
        self.client.publish(config.TOPIC_COMMAND, json.dumps(command_packet), qos=1)

    def run_automated_recipe_sequence(self):
        self.client.connect(config.BROKER_HOST, config.BROKER_PORT, config.KEEPALIVE_INTERVAL)
        self.client.loop_start()

        logger.info("Waiting for broker connection confirmation...")
        if not self.connected_event.wait(timeout=10.0):
            logger.error("Broker connection timed out. Aborting sequence.")
            self.client.loop_stop()
            return

        time.sleep(1.0)

        # Test Case 1: Recipe temp boost (72.0 C -> 78.5 C)
        self.send_command("CMD_101", "SET_TEMPERATURE", 78.5)
        time.sleep(3.0)

        # Test Case 2: Pump flow rate boost (25.0 LPM -> 32.0 LPM)
        self.send_command("CMD_102", "SET_FLOW_RATE", 32.0)
        time.sleep(3.0)

        # Test Case 3: Out-of-bounds safety rejection test (98.0 C)
        self.send_command("CMD_103", "SET_TEMPERATURE", 98.0)
        time.sleep(3.0)

        # Test Case 4: Operational state transition
        self.send_command("CMD_104", "SET_STATE", "CIP_CLEANING")
        time.sleep(3.0)

        logger.info("Sequence completed. Disconnecting dispatcher...")
        self.client.loop_stop()
        self.client.disconnect()


if __name__ == "__main__":
    dispatcher = RemoteCommandDispatcher()
    dispatcher.run_automated_recipe_sequence()