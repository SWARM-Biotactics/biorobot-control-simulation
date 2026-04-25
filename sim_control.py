from __future__ import annotations

import json
import math
import os
import random
import signal
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import paho.mqtt.client as mqtt

from config_runtime import ConfigStore, seeded_pose, random_jitter
from mqtt_topics import (
    CAMERA_STATUS_TOPIC,
    CONFIG_TOPIC_DEFAULT,
    ROBOTS_STATUS_TOPIC,
    sensor_compass_topic,
    sensor_movement_topic,
    sensor_position_topic,
)


MQTT_BROKER = os.getenv("MQTT_BROKER", os.getenv("MQTT_HOST", "mosquitto"))
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
CONFIG_TOPIC = os.getenv("CONFIG_TOPIC", CONFIG_TOPIC_DEFAULT)

SIM_LOOP_HZ = float(os.getenv("SIM_LOOP_HZ", "10"))
STATUS_HZ = float(os.getenv("STATUS_HZ", "2"))
CAMERA_STATUS_HZ = float(os.getenv("CAMERA_STATUS_HZ", "1"))

WORLD_MIN_X = float(os.getenv("WORLD_MIN_X", "0"))
WORLD_MAX_X = float(os.getenv("WORLD_MAX_X", "10000"))
WORLD_MIN_Y = float(os.getenv("WORLD_MIN_Y", "0"))
WORLD_MAX_Y = float(os.getenv("WORLD_MAX_Y", "10000"))

DEFAULT_SPEED = float(os.getenv("SIM_DEFAULT_SPEED", "220"))      # px/sec
TURN_RATE = float(os.getenv("SIM_TURN_RATE_DEG", "120"))          # deg/sec
DRAG = float(os.getenv("SIM_DRAG", "0.92"))
JITTER = float(os.getenv("SIM_JITTER", "4"))

ACTION_FORWARD_SECONDS = float(os.getenv("ACTION_FORWARD_SECONDS", "0.6"))
ACTION_TURN_SECONDS = float(os.getenv("ACTION_TURN_SECONDS", "0.45"))

CAMERA_ID = os.getenv("SIM_CAMERA_ID", "sim-camera-1")
CAMERA_NAME = os.getenv("SIM_CAMERA_NAME", "Indoor Arena Simulation Camera")


running = True


@dataclass
class ActionIntent:
    forward_until: float = 0.0
    turn_until: float = 0.0
    turn_dir: int = 0   # -1 left, +1 right, 0 none


@dataclass
class RobotState:
    uuid: str
    robot_id: str
    marker_id: str
    x: float
    y: float
    heading: float
    vx: float = 0.0
    vy: float = 0.0
    moving: bool = False
    intent: ActionIntent = field(default_factory=ActionIntent)

    def as_position_payload(self) -> dict[str, Any]:
        ts = int(time.time() * 1000)
        return {
            "timestamp": ts,
            "x": self.x,
            "y": self.y,
            "eventType": "stabilized",
            "heading": self.heading,
        }

    def as_compass_payload(self) -> dict[str, Any]:
        ts = int(time.time() * 1000)
        return {
            "timestamp": ts,
            "heading": self.heading,
            "eventType": "stabilized",
        }

    def as_movement_payload(self) -> dict[str, Any]:
        ts = int(time.time() * 1000)
        return {
            "timestamp": ts,
            "moving": self.moving,
            "movementState": 1 if self.moving else 0,
            "sourceTimestamp": float(ts),
        }

    def as_status_payload(self) -> dict[str, Any]:
        return {
            "id": self.robot_id,
            "x": int(round(self.x)),
            "y": int(round(self.y)),
            "status": "active",
        }


config_store = ConfigStore()
robot_states: dict[str, RobotState] = {}
state_lock = threading.Lock()


def log(msg: str) -> None:
    print(f"[sim-control] {time.strftime('%H:%M:%S')} {msg}", flush=True)


def wrap_angle(deg: float) -> float:
    return deg % 360.0


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def rebuild_states_from_config() -> None:
    with state_lock:
        existing = robot_states.copy()
        robot_states.clear()

        for idx, (uuid, cfg) in enumerate(sorted(config_store.runtime.robots.items())):
            if uuid in existing:
                prev = existing[uuid]
                robot_states[uuid] = RobotState(
                    uuid=uuid,
                    robot_id=cfg.robot_id,
                    marker_id=cfg.marker_id,
                    x=prev.x,
                    y=prev.y,
                    heading=prev.heading,
                    vx=prev.vx,
                    vy=prev.vy,
                    moving=prev.moving,
                    intent=prev.intent,
                )
            else:
                x, y, heading = seeded_pose(idx)
                robot_states[uuid] = RobotState(
                    uuid=uuid,
                    robot_id=cfg.robot_id,
                    marker_id=cfg.marker_id,
                    x=x,
                    y=y,
                    heading=heading,
                )

        summary = [
            f"{s.robot_id}:{s.uuid[:8]} marker={s.marker_id} ({int(s.x)},{int(s.y)})"
            for s in robot_states.values()
        ]
    log(f"loaded {len(robot_states)} robots from config -> {summary}")


def handle_action(robot_uuid: str, payload: dict[str, Any]) -> None:
    with state_lock:
        state = robot_states.get(robot_uuid)
        if not state:
            return

        action = str(payload.get("action", "")).lower()
        channel = int(payload.get("channel", -1))
        now = time.time()

        if action == "signal":
            if channel == 1:
                state.intent.forward_until = now + ACTION_FORWARD_SECONDS
                log(f"action forward robot={state.robot_id} channel=1")
            elif channel == 0:
                state.intent.turn_dir = -1
                state.intent.turn_until = now + ACTION_TURN_SECONDS
                log(f"action left robot={state.robot_id} channel=0")
            elif channel == 2:
                state.intent.turn_dir = 1
                state.intent.turn_until = now + ACTION_TURN_SECONDS
                log(f"action right robot={state.robot_id} channel=2")
            else:
                log(f"ignored signal with unknown channel={channel} robot={state.robot_id}")
        else:
            log(f"ignored unsupported action={action} robot={state.robot_id}")


def on_connect(client: mqtt.Client, userdata: Any, flags: dict, reason_code: Any, properties: Any = None) -> None:
    log(f"connected to mqtt rc={reason_code}")
    client.subscribe(CONFIG_TOPIC)
    client.subscribe("actions/biorobot/+/execute")


def on_message(client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
    topic = msg.topic

    if topic == CONFIG_TOPIC:
        try:
            config_store.update_from_payload(msg.payload)
            rebuild_states_from_config()
        except Exception as exc:
            log(f"failed to parse config: {exc}")
        return

    if topic.startswith("actions/biorobot/") and topic.endswith("/execute"):
        parts = topic.split("/")
        if len(parts) >= 4:
            robot_uuid = parts[2]
            try:
                payload = json.loads(msg.payload.decode("utf-8", errors="replace"))
            except Exception as exc:
                log(f"failed to decode action payload: {exc}")
                return
            handle_action(robot_uuid, payload)


def physics_loop(client: mqtt.Client) -> None:
    dt = 1.0 / max(1.0, SIM_LOOP_HZ)
    while running:
        now = time.time()
        with state_lock:
            for state in robot_states.values():
                if now < state.intent.turn_until and state.intent.turn_dir != 0:
                    state.heading = wrap_angle(state.heading + state.intent.turn_dir * TURN_RATE * dt)
                elif now >= state.intent.turn_until:
                    state.intent.turn_dir = 0

                if now < state.intent.forward_until:
                    rad = math.radians(state.heading)
                    accel = DEFAULT_SPEED
                    state.vx += math.cos(rad) * accel * dt
                    state.vy += math.sin(rad) * accel * dt

                state.vx *= DRAG
                state.vy *= DRAG

                state.vx += random_jitter(JITTER) * dt
                state.vy += random_jitter(JITTER) * dt

                state.x += state.vx * dt
                state.y += state.vy * dt

                bounced = False

                if state.x < WORLD_MIN_X:
                    state.x = WORLD_MIN_X
                    state.vx = abs(state.vx)
                    bounced = True
                elif state.x > WORLD_MAX_X:
                    state.x = WORLD_MAX_X
                    state.vx = -abs(state.vx)
                    bounced = True

                if state.y < WORLD_MIN_Y:
                    state.y = WORLD_MIN_Y
                    state.vy = abs(state.vy)
                    bounced = True
                elif state.y > WORLD_MAX_Y:
                    state.y = WORLD_MAX_Y
                    state.vy = -abs(state.vy)
                    bounced = True

                speed = math.hypot(state.vx, state.vy)
                state.moving = speed > 5.0

                if speed > 1.0:
                    state.heading = wrap_angle(math.degrees(math.atan2(state.vy, state.vx)))

                if bounced:
                    log(f"bounce robot={state.robot_id} pos=({int(state.x)},{int(state.y)})")

                client.publish(sensor_position_topic(state.uuid), json.dumps(state.as_position_payload()))
                client.publish(sensor_compass_topic(state.uuid), json.dumps(state.as_compass_payload()))
                client.publish(sensor_movement_topic(state.uuid), json.dumps(state.as_movement_payload()))

        time.sleep(dt)


def robots_status_loop(client: mqtt.Client) -> None:
    dt = 1.0 / max(0.1, STATUS_HZ)
    while running:
        with state_lock:
            payload = [state.as_status_payload() for state in robot_states.values()]
        client.publish(ROBOTS_STATUS_TOPIC, json.dumps(payload), retain=False)
        time.sleep(dt)


def camera_status_loop(client: mqtt.Client) -> None:
    dt = 1.0 / max(0.1, CAMERA_STATUS_HZ)
    while running:
        payload = {
            "cameras": [
                {
                    "id": CAMERA_ID,
                    "name": CAMERA_NAME,
                    "status": "online",
                    "source": "simulation",
                    "fps": SIM_LOOP_HZ,
                    "updated_at": int(time.time() * 1000),
                }
            ]
        }
        client.publish(CAMERA_STATUS_TOPIC, json.dumps(payload), retain=False)
        time.sleep(dt)


def stop_handler(signum: int, frame: Any) -> None:
    global running
    running = False
    log("shutdown requested")


def main() -> None:
    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    log(f"connecting to mqtt://{MQTT_BROKER}:{MQTT_PORT} config_topic={CONFIG_TOPIC}")
    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()

    threading.Thread(target=physics_loop, args=(client,), daemon=True).start()
    threading.Thread(target=robots_status_loop, args=(client,), daemon=True).start()
    threading.Thread(target=camera_status_loop, args=(client,), daemon=True).start()

    while running:
        time.sleep(0.5)

    client.loop_stop()
    client.disconnect()
    log("stopped")


if __name__ == "__main__":
    main()