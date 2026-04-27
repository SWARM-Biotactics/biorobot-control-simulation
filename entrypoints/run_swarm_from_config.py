from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from typing import Any

import paho.mqtt.client as mqtt


MQTT_HOST = os.getenv("MQTT_HOST", os.getenv("MQTT_BROKER", "mosquitto"))
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
CONFIG_TOPIC = os.getenv("CONFIG_TOPIC", "config")
BOOTSTRAP_TIMEOUT = float(os.getenv("BOOTSTRAP_TIMEOUT", "20"))

running = True
children: list[subprocess.Popen[str]] = []


def log(msg: str) -> None:
    print(f"[run-swarm-from-config] {time.strftime('%H:%M:%S')} {msg}", flush=True)


def parse_payload(payload: bytes) -> dict[str, Any]:
    return json.loads(payload.decode("utf-8", errors="replace"))


def extract_robot_uuids(cfg: dict[str, Any]) -> list[str]:
    robots = cfg.get("biorobots") or []
    master_enable = bool((cfg.get("master_enable") or {}).get("enable_swarm_controllers", False))

    uuids: list[str] = []
    seen: set[str] = set()

    for item in robots:
        uuid = str(item.get("uuid") or "").strip()
        if not uuid or uuid in seen:
            continue

        per_robot = item.get("enable_swarm_controller", None)
        enabled = bool(per_robot) if per_robot is not None else master_enable
        if not enabled:
            continue

        seen.add(uuid)
        uuids.append(uuid)

    return uuids


def wait_for_retained_config() -> dict[str, Any]:
    received: dict[str, Any] | None = None

    def on_connect(
        client: mqtt.Client,
        userdata: Any,
        flags: dict,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        log(f"connected to mqtt rc={reason_code}, subscribing to {CONFIG_TOPIC}")
        client.subscribe(CONFIG_TOPIC)

    def on_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        nonlocal received
        if msg.topic != CONFIG_TOPIC:
            return
        try:
            received = parse_payload(msg.payload)
            log("received retained config payload")
        except Exception as exc:
            log(f"failed to parse config payload: {exc}")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()
    try:
        deadline = time.time() + BOOTSTRAP_TIMEOUT
        while received is None:
            if time.time() > deadline:
                raise TimeoutError(
                    f"Timed out waiting for retained config on topic '{CONFIG_TOPIC}'"
                )
            time.sleep(0.1)
    finally:
        client.loop_stop()
        client.disconnect()

    return received


def spawn_controller(uuid: str) -> subprocess.Popen[str]:
    cmd = [
        sys.executable,
        "entrypoints/run_swarm.py",
        "--biorobot-uuid",
        uuid,
        "--bootstrap-mqtt-host",
        MQTT_HOST,
        "--bootstrap-mqtt-port",
        str(MQTT_PORT),
        "--bootstrap-config-topic",
        CONFIG_TOPIC,
        "--bootstrap-timeout",
        str(int(BOOTSTRAP_TIMEOUT)),
    ]
    log(f"starting swarm controller for uuid={uuid}")
    return subprocess.Popen(cmd)


def stop_children() -> None:
    for child in children:
        if child.poll() is None:
            try:
                child.terminate()
            except Exception:
                pass

    deadline = time.time() + 10
    while time.time() < deadline:
        alive = [c for c in children if c.poll() is None]
        if not alive:
            return
        time.sleep(0.2)

    for child in children:
        if child.poll() is None:
            try:
                child.kill()
            except Exception:
                pass


def handle_stop(signum: int, frame: Any) -> None:
    global running
    running = False
    log("shutdown requested")
    stop_children()


def main() -> None:
    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    cfg = wait_for_retained_config()
    uuids = extract_robot_uuids(cfg)

    if not uuids:
        raise RuntimeError("No swarm-enabled robot UUIDs found in retained config")

    log(f"found {len(uuids)} swarm-enabled robots: {uuids}")

    for uuid in uuids:
        children.append(spawn_controller(uuid))

    while running:
        for child in children:
            rc = child.poll()
            if rc is not None:
                raise RuntimeError(f"Swarm controller exited unexpectedly with code {rc}")
        time.sleep(1.0)

    stop_children()


if __name__ == "__main__":
    main()