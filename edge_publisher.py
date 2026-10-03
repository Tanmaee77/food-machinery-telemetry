"""
Edge Node Process Simulator: Pasteurizer Unit 01
Simulates industrial transmitter telemetry, setpoint controls, and LWT.
"""

import json
import logging
import random
import signal
import sys
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

import config

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("EdgeNode")


class PasteurizerEdgeNode:
    def __init__(self):
        # Process State & Initial HTST (High-Temperature Short-Time) Setpoints
        self.target_temperature = 72.0  # °C
        self.target_flow_rate = 25.0    # Liters Per Minute (LPM)
        self.tank_level = 68.5          # %
        self.heater_duty = 48.0         # 0 - 100%
        self.machine_state = "RUNNING"
        self.running = True

        # Initialize MQTT Client using modern Paho v2 callback API
        self.client = mqtt.Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=f"EdgeSimulator_{config.ASSET}",
            clean_session=True
        )

        # Wire event callbacks
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

        # Configure Last Will and Testament (LWT)
        lwt_payload = json.dumps({
            "asset_id": config.ASSET,
            "status": config.STATUS_OFFLINE_FAULT,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        self.client.will_set(
            topic=config.TOPIC_STATUS,
            payload=lwt_payload,
            qos=1,
            retain=True
        )

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("Connected to MQTT Broker at %s:%d", config.BROKER_HOST, config.BROKER_PORT)

            # Publish ONLINE status with retain=True for immediate state discovery
            online_payload = json.dumps({
                "asset_id": config.ASSET,
                "status": config.STATUS_ONLINE,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            self.client.publish(config.TOPIC_STATUS, online_payload, qos=1, retain=True)

            # Subscribe to remote recipe and control commands
            self.client.subscribe(config.TOPIC_COMMAND, qos=1)
            logger.info("Subscribed to command channel: %s", config.TOPIC_COMMAND)
        else:
            logger.error("Connection failed with code: %s", reason_code)

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        logger.warning("Disconnected from broker. Reason: %s", reason_code)

    def on_message(self, client, userdata, msg):
        """Processes remote supervisory commands and recipe setpoint updates."""
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            logger.info("Received command payload: %s", payload)

            cmd_id = payload.get("command_id", "CMD_UNKNOWN")
            command = payload.get("command")
            value = payload.get("value")

            success = False
            ack_msg = ""

            # Validate against physical safety and engineering boundaries
            if command == "SET_TEMPERATURE":
                if 60.0 <= value <= 90.0:
                    self.target_temperature = float(value)
                    success = True
                    ack_msg = f"Target temperature set to {self.target_temperature:.1f} C"
                else:
                    ack_msg = "Requested temperature violates safety limits (60-90 C)"

            elif command == "SET_FLOW_RATE":
                if 10.0 <= value <= 50.0:
                    self.target_flow_rate = float(value)
                    success = True
                    ack_msg = f"Target flow rate set to {self.target_flow_rate:.1f} LPM"
                else:
                    ack_msg = "Requested flow rate out of pump operating range (10-50 LPM)"

            elif command == "SET_STATE":
                if value in ["RUNNING", "STANDBY", "CIP_CLEANING"]:
                    self.machine_state = value
                    success = True
                    ack_msg = f"Machine state shifted to {self.machine_state}"
                else:
                    ack_msg = f"Invalid operational state: {value}"
            else:
                ack_msg = f"Unrecognized command: {command}"

            # Dispatch confirmation acknowledgment
            ack_payload = json.dumps({
                "command_id": cmd_id,
                "asset_id": config.ASSET,
                "success": success,
                "status_message": ack_msg,
                "applied_state": {
                    "target_temp": self.target_temperature,
                    "target_flow": self.target_flow_rate,
                    "state": self.machine_state
                },
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            self.client.publish(config.TOPIC_COMMAND_ACK, ack_payload, qos=1)
            logger.info("Dispatched ACK: %s", ack_payload)

        except json.JSONDecodeError:
            logger.error("Corrupted command JSON: %s", msg.payload)
        except Exception as e:
            logger.exception("Error processing supervisory command: %s", e)

    def _simulate_process_dynamics(self):
        """Simulates physical sensor noise and dynamic actuator responses."""
        noise_temp = random.gauss(0, 0.18)
        noise_flow = random.gauss(0, 0.25)
        level_drift = random.uniform(-0.1, 0.1)

        sim_temp = round(self.target_temperature + noise_temp, 2)
        sim_flow = round(self.target_flow_rate + noise_flow, 2)
        self.tank_level = max(5.0, min(95.0, round(self.tank_level + level_drift, 2)))

        # Simple proportional heater control duty adjustment
        error = self.target_temperature - sim_temp
        self.heater_duty = max(0.0, min(100.0, round(50.0 + (error * 12.0), 1)))

        return sim_temp, sim_flow, self.tank_level, self.heater_duty

    def start(self):
        self.client.connect(config.BROKER_HOST, config.BROKER_PORT, config.KEEPALIVE_INTERVAL)
        self.client.loop_start()

        logger.info("Starting telemetry transmission loop (Interval: 1.0s)...")
        try:
            while self.running:
                temp, flow, level, duty = self._simulate_process_dynamics()

                telemetry_packet = {
                    "asset_id": config.ASSET,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "process_values": {
                        "temperature_pv": temp,
                        "flow_rate_pv": flow,
                        "tank_level_pv": level
                    },
                    "setpoints": {
                        "temperature_sp": self.target_temperature,
                        "flow_rate_sp": self.target_flow_rate
                    },
                    "actuators": {
                        "heater_duty_percent": duty
                    },
                    "operational_state": self.machine_state
                }

                self.client.publish(
                    config.TOPIC_TELEMETRY,
                    json.dumps(telemetry_packet),
                    qos=0
                )
                time.sleep(1.0)

        except KeyboardInterrupt:
            self.shutdown()

    def shutdown(self):
        logger.info("Executing graceful node shutdown...")
        self.running = False

        # Clear fault LWT with an explicit OFFLINE message
        graceful_payload = json.dumps({
            "asset_id": config.ASSET,
            "status": config.STATUS_OFFLINE_GRACEFUL,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
        self.client.publish(config.TOPIC_STATUS, graceful_payload, qos=1, retain=True)
        time.sleep(0.5)

        self.client.loop_stop()
        self.client.disconnect()
        logger.info("Shutdown completed. Disconnected from broker.")


if __name__ == "__main__":
    node = PasteurizerEdgeNode()

    # Capture termination signals for clean shutdown
    signal.signal(signal.SIGINT, lambda sig, frame: node.shutdown() or sys.exit(0))
    signal.signal(signal.SIGTERM, lambda sig, frame: node.shutdown() or sys.exit(0))

    node.start()