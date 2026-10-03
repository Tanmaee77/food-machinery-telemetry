"""
Live Terminal Industrial HMI / Dashboard
Subscribes to telemetry and ACK feeds, rendering a real-time terminal display.
"""

import json
import time
from datetime import datetime
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.progress import BarColumn, Progress, TextColumn
from rich.table import Table
from rich.text import Text

import config

console = Console()

# Global state cache updated by MQTT callbacks
state = {
    "status": "CONNECTING...",
    "state": "INITIALIZING",
    "temp_pv": 0.0,
    "temp_sp": 72.0,
    "flow_pv": 0.0,
    "flow_sp": 25.0,
    "level_pv": 0.0,
    "heater_duty": 0.0,
    "last_seen": "Never",
    "recent_events": []
}


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        state["status"] = "ONLINE"
        client.subscribe(config.TOPIC_TELEMETRY, qos=0)
        client.subscribe(config.TOPIC_COMMAND_ACK, qos=1)
        client.subscribe(config.TOPIC_STATUS, qos=1)
    else:
        state["status"] = f"ERROR ({reason_code})"


def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = json.loads(msg.payload.decode("utf-8"))

        if topic == config.TOPIC_TELEMETRY:
            pvs = payload.get("process_values", {})
            sps = payload.get("setpoints", {})
            acts = payload.get("actuators", {})

            state["temp_pv"] = pvs.get("temperature_pv", 0.0)
            state["flow_pv"] = pvs.get("flow_rate_pv", 0.0)
            state["level_pv"] = pvs.get("tank_level_pv", 0.0)
            state["temp_sp"] = sps.get("temperature_sp", 72.0)
            state["flow_sp"] = sps.get("flow_rate_sp", 25.0)
            state["heater_duty"] = acts.get("heater_duty_percent", 0.0)
            state["state"] = payload.get("operational_state", "RUNNING")
            state["last_seen"] = datetime.now().strftime("%H:%M:%S")

        elif topic == config.TOPIC_COMMAND_ACK:
            success = "SUCCESS" if payload.get("success") else "REJECTED"
            cmd_id = payload.get("command_id", "CMD")
            msg_txt = payload.get("status_message", "")
            t_stamp = datetime.now().strftime("%H:%M:%S")
            state["recent_events"].insert(0, f"[{t_stamp}] [{success}] {cmd_id}: {msg_txt}")
            state["recent_events"] = state["recent_events"][:5]

        elif topic == config.TOPIC_STATUS:
            state["status"] = payload.get("status", "ONLINE")

    except Exception:
        pass


def build_gauge_bar(value: float, min_val: float, max_val: float, unit: str, color: str = "cyan") -> Progress:
    progress = Progress(
        TextColumn(f"[bold]{value:>5.1f} {unit}[/]"),
        BarColumn(bar_width=25, complete_style=color, finished_style=color),
        expand=False
    )
    task_id = progress.add_task("val", total=(max_val - min_val))
    progress.update(task_id, completed=max(0.0, min(max_val - min_val, value - min_val)))
    return progress


def render_dashboard() -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body", size=13),
        Layout(name="events", size=8)
    )

    # Header Panel
    status_color = "green" if state["status"] == "ONLINE" else "red"
    header_text = Text.assemble(
        ("ISA-95 Plant Asset: ", "bold white"),
        (f"{config.FACILITY} / {config.LINE} / {config.ASSET}    ", "bold yellow"),
        ("Broker: ", "bold white"),
        (f"{state['status']}    ", f"bold {status_color}"),
        ("State: ", "bold white"),
        (f"{state['state']}    ", "bold cyan"),
        ("Heartbeat: ", "bold white"),
        (f"{state['last_seen']}", "bold magenta")
    )
    layout["header"].update(Panel(header_text, style="blue", title="[bold]SCADA Edge Monitor[/bold]"))

    # Body Table (Process Variables & Setpoints)
    body_table = Table(expand=True, box=None)
    body_table.add_column("Measurement Variable", justify="left", style="bold")
    body_table.add_column("Live Gauge (Range)", justify="left")
    body_table.add_column("Process Value (PV)", justify="center", style="bold yellow")
    body_table.add_column("Setpoint (SP)", justify="center", style="bold cyan")
    body_table.add_column("Variance (Error)", justify="center")

    # Temp Row
    temp_err = state["temp_pv"] - state["temp_sp"]
    temp_color = "green" if abs(temp_err) < 1.0 else "red"
    body_table.add_row(
        "Pasteurization Temp (°C)",
        build_gauge_bar(state["temp_pv"], 50.0, 100.0, "°C", color="red"),
        f"{state['temp_pv']:.2f} °C",
        f"{state['temp_sp']:.1f} °C",
        f"[{temp_color}]{temp_err:+.2f} °C[/]"
    )

    # Flow Row
    flow_err = state["flow_pv"] - state["flow_sp"]
    flow_color = "green" if abs(flow_err) < 1.5 else "yellow"
    body_table.add_row(
        "Product Flow Rate (LPM)",
        build_gauge_bar(state["flow_pv"], 0.0, 60.0, "LPM", color="cyan"),
        f"{state['flow_pv']:.2f} LPM",
        f"{state['flow_sp']:.1f} LPM",
        f"[{flow_color}]{flow_err:+.2f} LPM[/]"
    )

    # Level Row
    body_table.add_row(
        "Buffer Tank Level (%)",
        build_gauge_bar(state["level_pv"], 0.0, 100.0, "%", color="blue"),
        f"{state['level_pv']:.1f} %",
        "N/A",
        "[white]Nominal[/]"
    )

    # Heater Duty Row
    body_table.add_row(
        "Heater Duty Output (%)",
        build_gauge_bar(state["heater_duty"], 0.0, 100.0, "%", color="magenta"),
        f"{state['heater_duty']:.1f} %",
        "AUTO",
        "[magenta]Active PID/PWM[/]"
    )

    layout["body"].update(Panel(body_table, title="[bold]Transmitter & Actuator Telemetry[/bold]", style="cyan"))

    # Event Feed Panel
    events_text = Text()
    if not state["recent_events"]:
        events_text.append("Awaiting supervisory commands or alarm events...", style="dim")
    else:
        for ev in state["recent_events"]:
            if "[SUCCESS]" in ev:
                events_text.append(ev + "\n", style="green")
            else:
                events_text.append(ev + "\n", style="bold red")

    layout["events"].update(Panel(events_text, title="[bold]Supervisory Control Event Feed[/bold]", style="white"))

    return layout


def main():
    client = mqtt.Client(
        callback_api_version=CallbackAPIVersion.VERSION2,
        client_id="Terminal_HMI_Dashboard",
        clean_session=True
    )
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(config.BROKER_HOST, config.BROKER_PORT, config.KEEPALIVE_INTERVAL)
    client.loop_start()

    with Live(render_dashboard(), refresh_per_second=2, screen=True) as live:
        try:
            while True:
                live.update(render_dashboard())
                time.sleep(0.5)
        except KeyboardInterrupt:
            client.loop_stop()
            client.disconnect()


if __name__ == "__main__":
    main()