"""
Industrial Telemetry Pipeline Configuration
Defines MQTT broker parameters, topic hierarchies, and status constants.
"""

# Public test broker for development and prototyping
BROKER_HOST = "test.mosquitto.org"
BROKER_PORT = 1883
KEEPALIVE_INTERVAL = 30  # seconds

# Enterprise / Plant Hierarchy (ISA-95 compliant model)
FACILITY = "blr_plant01"
LINE = "beverage_line"
ASSET = "pasteurizer_01"

BASE_TOPIC = f"factory/{FACILITY}/{LINE}/{ASSET}"

# Topic Definitions
TOPIC_TELEMETRY = f"{BASE_TOPIC}/telemetry"
TOPIC_STATUS = f"{BASE_TOPIC}/status"          # LWT and operational state
TOPIC_COMMAND = f"{BASE_TOPIC}/cmd"            # Remote recipe setpoints
TOPIC_COMMAND_ACK = f"{BASE_TOPIC}/cmd/ack"    # Bidirectional command acknowledgment

# Machine Status Strings
STATUS_ONLINE = "ONLINE"
STATUS_OFFLINE_GRACEFUL = "OFFLINE"
STATUS_OFFLINE_FAULT = "FAULT_DISCONNECTED"