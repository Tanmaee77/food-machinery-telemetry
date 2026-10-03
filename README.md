# Industrial IoT Telemetry & Supervisory Edge Pipeline (ISA-95)

An end-to-end industrial IoT telemetry and supervisory control pipeline modeling an industrial beverage pasteurizer. The system follows ISA-95 asset hierarchy standards, implements store-and-forward SQLite persistence, provides closed-loop remote setpoint control with safety interlocks, and renders a live terminal-based SCADA HMI.

Architecture Overview

[ Beverage Pasteurizer ]  (edge_publisher.py)
       │  (MQTT: Telemetry Stream)
       ├──► [ Live SCADA Dashboard ] (dashboard.py)
       │
       ├──► [ Local SQLite Store ] (edge_logger.py)
       │         │
       │         ▼ (Batch Sync / QoS 1)
       │    [ Cloud Forwarder ] (cloud_forwarder.py) ──► Enterprise Ingestion
       │
       ▲  (MQTT: Commands & Safety Interlocks)
       │
[ Supervisory Dispatcher ] (command_dispatcher.py)

Live SCADA Interface
![SCADA Dashboard](docs/images/scada_dashboard.png)
Project Structure
food-machinery-telemetry/
│
├── config.py                 # Central ISA-95 topic schema and operational thresholds
├── edge_publisher.py         # Process simulation, telemetry publisher & command handler
├── edge_logger.py            # Store-and-forward local SQLite persistence engine
├── command_dispatcher.py     # Synchronized supervisory recipe controller
├── dashboard.py              # Live Rich terminal SCADA monitor
├── cloud_forwarder.py        # Edge-to-cloud batch synchronizer
├── data/
│   └── telemetry.db          # Local SQLite storage
├── docs/
│   └── images/
│       └── scada_dashboard.png
├── .gitignore
├── README.md
└── requirements.txt

Getting Started
1. Environmental Setup
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

2. Execution Order
python edge_publisher.py
python edge_logger.py
python dashboard.py
python command_dispatcher.py
python cloud_forwarder.py

