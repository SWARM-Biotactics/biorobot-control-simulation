from __future__ import annotations

import json
import os
import signal
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import paho.mqtt.client as mqtt

from config_runtime import ConfigStore, seeded_pose
from plant_model import apply_signal_to_robot, step_robot


MQTT_BROKER = os.getenv("MQTT_BROKER", os.getenv("MQTT_HOST", "mosquitto"))
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
CONFIG_TOPIC = os.getenv("CONFIG_TOPIC", "config")
SIM_STATE_TOPIC = os.getenv("SIM_STATE_TOPIC", "sim/robots/state")

SIM_LOOP_HZ = float(os.getenv("SIM_LOOP_HZ", "20"))

WORLD_MIN_X = float(os.getenv("WORLD_MIN_X", "800"))
WORLD_MAX_X = float(os.getenv("WORLD_MAX_X", "9200"))
WORLD_MIN_Y = float(os.getenv("WORLD_MIN_Y", "800"))
WORLD_MAX_Y = float(os.getenv("WORLD_MAX_Y", "9200"))

DEFAULT_SPEED = float(os.getenv("SIM_DEFAULT_SPEED", "280"))
TURN_RATE = float(os.getenv("SIM_TURN_RATE_DEG", "145"))
DRAG = float(os.getenv("SIM_DRAG", "0.99"))
JITTER = float(os.getenv("SIM_JITTER", "0.3"))

running = True


@dataclass
class ActionIntent:
    forward_until: float = 0.0
    turn_until: float = 0.0
    turn_dir: int = 0


@dataclass
class RobotState:
    uuid: str
    robot_id: str
    marker_id: str
    animal_id: str
    x: float
    y: float
    heading: float
    vx: float = 0.0
    vy: float = 0.0
    moving: bool = False
    intent: ActionIntent = field(default_factory=ActionIntent)

    def as_sim_payload(self) -> dict[str, Any]:
        return {
            "uuid": self.uuid,
            "robot_id": self.robot_id,
            "marker_id": self.marker_id,
            "animal_id": self.animal_id,
            "x": self.x,
            "y": self.y,
            "heading": self.heading,
            "vx": self.vx,
            "vy": self.vy,
            "moving": self.moving,
            "timestamp": int(time.time() * 1000),
        }


config_store = ConfigStore()
robot_states: dict[str, RobotState] = {}
state_lock = threading.Lock()


def log(msg: str) -> None:
    print(f"[sim-control] {time.strftime('%H:%M:%S')} {msg}", flush=True)


def rebuild_states_from_config() -> None:
    with state_lock:
        previous = robot_states.copy()
        robot_states.clear()

        items = sorted(config_store.runtime.robots.items())
        total = len(items)

        for idx, (uuid, cfg) in enumerate(items):
            if uuid in previous:
                old = previous[uuid]
                robot_states[uuid] = RobotState(
                    uuid=uuid,
                    robot_id=cfg.robot_id,
                    marker_id=cfg.marker_id,
                    animal_id=cfg.animal_id,
                    x=old.x,
                    y=old.y,
                    heading=old.heading,
                    vx=old.vx,
                    vy=old.vy,
                    moving=old.moving,
                    intent=old.intent,
                )
            else:
                x, y, heading = seeded_pose(idx, total)
                robot_states[uuid] = RobotState(
                    uuid=uuid,
                    robot_id=cfg.robot_id,
                    marker_id=cfg.marker_id,
                    animal_id=cfg.animal_id,
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

        action = str(payload.get("action", "")).strip()

        if action != "Signal":
            log(f"[actions] ignoring non-Signal action={action} robot={state.robot_id}")
            return

        channel = int(payload.get("channel", -1))
        amplitude_factor = float(payload.get("amplitudeFactor", 0.6))
        frequency = float(payload.get("frequency", 40))
        duration_ms = float(payload.get("durationMs", 500))

        log(
            f"[plant] Signal recv uuid={state.uuid} ch={channel} "
            f"ampFactor={amplitude_factor} f={frequency} durMs={duration_ms}"
        )
        apply_signal_to_robot(state, channel, amplitude_factor, frequency, duration_ms)


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


def physics_loop() -> None:
    dt = 1.0 / max(1.0, SIM_LOOP_HZ)
    bounds = (WORLD_MIN_X, WORLD_MAX_X, WORLD_MIN_Y, WORLD_MAX_Y)

    while running:
        with state_lock:
            for state in robot_states.values():
                hit_wall = step_robot(
                    state=state,
                    dt=dt,
                    bounds=bounds,
                    default_speed=DEFAULT_SPEED,
                    turn_rate_deg=TURN_RATE,
                    drag=DRAG,
                    jitter=JITTER,
                )
                if hit_wall:
                    log(f"[physics] wall hit robot={state.robot_id} pos=({state.x:.1f},{state.y:.1f})")
        time.sleep(dt)


def publish_sim_state_loop(client: mqtt.Client) -> None:
    dt = 1.0 / max(1.0, SIM_LOOP_HZ)
    while running:
        with state_lock:
            payload = [state.as_sim_payload() for state in robot_states.values()]
        client.publish(SIM_STATE_TOPIC, json.dumps(payload), retain=False)
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

    threading.Thread(target=physics_loop, daemon=True).start()
    threading.Thread(target=publish_sim_state_loop, args=(client,), daemon=True).start()

    while running:
        time.sleep(0.5)

    client.loop_stop()
    client.disconnect()
    log("stopped")


if __name__ == "__main__":
    main()