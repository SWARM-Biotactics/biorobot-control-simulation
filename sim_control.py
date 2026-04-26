from __future__ import annotations

import json
import math
import os
import signal
import threading
import time
from dataclasses import dataclass, field
from typing import Any

import paho.mqtt.client as mqtt

from angles import heading_to_yaw, yaw_diff_to_target
from config_runtime import ConfigStore, seeded_pose, random_jitter
from controller_history import BiorobotHistory
from mqtt_topics import CONFIG_TOPIC_DEFAULT, SIM_STATE_TOPIC_DEFAULT


MQTT_BROKER = os.getenv("MQTT_BROKER", os.getenv("MQTT_HOST", "mosquitto"))
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
CONFIG_TOPIC = os.getenv("CONFIG_TOPIC", CONFIG_TOPIC_DEFAULT)
SIM_STATE_TOPIC = os.getenv("SIM_STATE_TOPIC", SIM_STATE_TOPIC_DEFAULT)

SIM_LOOP_HZ = float(os.getenv("SIM_LOOP_HZ", "20"))
CONTROL_LOOP_HZ = float(os.getenv("CONTROL_LOOP_HZ", "8"))

WORLD_MIN_X = float(os.getenv("WORLD_MIN_X", "0"))
WORLD_MAX_X = float(os.getenv("WORLD_MAX_X", "10000"))
WORLD_MIN_Y = float(os.getenv("WORLD_MIN_Y", "0"))
WORLD_MAX_Y = float(os.getenv("WORLD_MAX_Y", "10000"))

DEFAULT_SPEED = float(os.getenv("SIM_DEFAULT_SPEED", "260"))
TURN_RATE = float(os.getenv("SIM_TURN_RATE_DEG", "120"))
DRAG = float(os.getenv("SIM_DRAG", "0.96"))
JITTER = float(os.getenv("SIM_JITTER", "2"))

ACTION_FORWARD_SECONDS = float(os.getenv("ACTION_FORWARD_SECONDS", "0.9"))
ACTION_TURN_SECONDS = float(os.getenv("ACTION_TURN_SECONDS", "0.5"))

MIN_TIME_BETWEEN_SIGNALS = float(os.getenv("MIN_TIME_BETWEEN_SIGNALS", "0.35"))
ANGLE_TOLERANCE = float(os.getenv("ANGLE_TOLERANCE", "30"))
ANGLE_TOLERANCE_BROAD = float(os.getenv("ANGLE_TOLERANCE_BROAD", "60"))
KEEP_MOVING_ENABLED = os.getenv("KEEP_MOVING_ENABLED", "true").lower() == "true"

CHANNEL_BOTH_CERCI = 0
CHANNEL_LEFT_ANTENNA = 1
CHANNEL_RIGHT_ANTENNA = 2
CHANNEL_LEFT_CERCUS = 3
CHANNEL_RIGHT_CERCUS = 4
CHANNEL_BOTH_ANTENNAE = 5

running = True


@dataclass
class ActionIntent:
    forward_until: float = 0.0
    turn_until: float = 0.0
    turn_dir: int = 0
    source: str = ""


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
    target_heading: float | None = None
    intent: ActionIntent = field(default_factory=ActionIntent)
    history: BiorobotHistory = field(default_factory=BiorobotHistory)
    last_signal_log_ts: float = 0.0

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
            "target_heading": self.target_heading,
            "timestamp": int(time.time() * 1000),
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
        prev = robot_states.copy()
        robot_states.clear()

        items = sorted(config_store.runtime.robots.items())
        total = len(items)

        for idx, (uuid, cfg) in enumerate(items):
            if uuid in prev:
                old = prev[uuid]
                state = RobotState(
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
                    target_heading=old.target_heading,
                    intent=old.intent,
                    history=old.history,
                    last_signal_log_ts=old.last_signal_log_ts,
                )
            else:
                x, y, heading = seeded_pose(idx, total)
                state = RobotState(
                    uuid=uuid,
                    robot_id=cfg.robot_id,
                    marker_id=cfg.marker_id,
                    animal_id=cfg.animal_id,
                    x=x,
                    y=y,
                    heading=heading,
                )

            if config_store.runtime.target_heading is not None:
                state.target_heading = config_store.runtime.target_heading

            robot_states[uuid] = state

        summary = [
            f"{s.robot_id}:{s.uuid[:8]} marker={s.marker_id} ({int(s.x)},{int(s.y)})"
            for s in robot_states.values()
        ]
    log(f"loaded {len(robot_states)} robots from config -> {summary}")


def apply_signal_to_robot(state: RobotState, channel: int, amplitude_factor: float, frequency: float, duration_ms: float, source: str) -> None:
    now = time.time()
    duration_s = max(0.05, float(duration_ms) / 1000.0)

    amp = clamp(amplitude_factor, 0.182, 1.0)
    abs_amp = max(1000.0, min(amp * 6600.0, 6600.0))
    state.history.add_signal(channel, frequency, abs_amp, duration_ms)

    if channel == CHANNEL_LEFT_ANTENNA:
        state.intent.turn_dir = 1
        state.intent.turn_until = now + duration_s
        state.intent.source = source
        log(f"[controller] {time.strftime('%H:%M:%S')} INFO: Sending signal LEFT_ANTENNA: {frequency}, {abs_amp}, {int(duration_ms)} to backpack sim with marker {state.marker_id}")
    elif channel == CHANNEL_RIGHT_ANTENNA:
        state.intent.turn_dir = -1
        state.intent.turn_until = now + duration_s
        state.intent.source = source
        log(f"[controller] {time.strftime('%H:%M:%S')} INFO: Sending signal RIGHT_ANTENNA: {frequency}, {abs_amp}, {int(duration_ms)} to backpack sim with marker {state.marker_id}")
    elif channel == CHANNEL_BOTH_CERCI:
        state.intent.forward_until = now + duration_s
        state.intent.source = source
        log(f"[controller] {time.strftime('%H:%M:%S')} INFO: Sending signal BOTH_CERCI: {frequency}, {abs_amp}, {int(duration_ms)} to backpack sim with marker {state.marker_id}")
    elif channel == CHANNEL_LEFT_CERCUS:
        state.intent.turn_dir = 1
        state.intent.turn_until = now + duration_s * 0.7
        state.intent.source = source
        log(f"[controller] {time.strftime('%H:%M:%S')} INFO: Sending signal LEFT_CERCUS: {frequency}, {abs_amp}, {int(duration_ms)} to backpack sim with marker {state.marker_id}")
    elif channel == CHANNEL_RIGHT_CERCUS:
        state.intent.turn_dir = -1
        state.intent.turn_until = now + duration_s * 0.7
        state.intent.source = source
        log(f"[controller] {time.strftime('%H:%M:%S')} INFO: Sending signal RIGHT_CERCUS: {frequency}, {abs_amp}, {int(duration_ms)} to backpack sim with marker {state.marker_id}")
    elif channel == CHANNEL_BOTH_ANTENNAE:
        state.intent.forward_until = now + duration_s
        state.intent.source = source
        log(f"[controller] {time.strftime('%H:%M:%S')} INFO: Sending signal BOTH_ANTENNAE: {frequency}, {abs_amp}, {int(duration_ms)} to backpack sim with marker {state.marker_id}")


def handle_action(robot_uuid: str, payload: dict[str, Any]) -> None:
    with state_lock:
        state = robot_states.get(robot_uuid)
        if not state:
            return

        action = str(payload.get("action", "")).strip()

        if action == "GoToHeading":
            th = payload.get("target_heading")
            try:
                state.target_heading = float(th) % 360.0
                log(f"[actions] target heading override robot={state.robot_id} -> {state.target_heading:.2f}")
            except Exception:
                log(f"[actions] invalid target_heading payload for robot={state.robot_id}: {th!r}")
            return

        if action != "Signal":
            log(f"[actions] ignored unsupported action={action} robot={state.robot_id}")
            return

        channel = int(payload.get("channel", -1))
        amplitude_factor = float(payload.get("amplitudeFactor", 0.6))
        frequency = float(payload.get("frequency", 40))
        duration_ms = float(payload.get("durationMs", 500))

        log(
            f"[connector] ACTION recv uuid={state.uuid} ch={channel} "
            f"ampFactor={amplitude_factor} amp={amplitude_factor*6600:.0f} "
            f"f={frequency} durMs={duration_ms}"
        )

        # Immediate manual response
        apply_signal_to_robot(state, channel, amplitude_factor, frequency, duration_ms, source="manual")

        # Also update desired heading so controller_loop can continue like original repo
        if channel == CHANNEL_LEFT_ANTENNA:
            state.target_heading = wrap_angle(state.heading + 35.0)
            log(f"[actions] manual LEFT_ANTENNA -> target heading {state.target_heading:.2f} for {state.robot_id}")
        elif channel == CHANNEL_RIGHT_ANTENNA:
            state.target_heading = wrap_angle(state.heading - 35.0)
            log(f"[actions] manual RIGHT_ANTENNA -> target heading {state.target_heading:.2f} for {state.robot_id}")
        elif channel == CHANNEL_LEFT_CERCUS:
            state.target_heading = wrap_angle(state.heading + 20.0)
            log(f"[actions] manual LEFT_CERCUS -> target heading {state.target_heading:.2f} for {state.robot_id}")
        elif channel == CHANNEL_RIGHT_CERCUS:
            state.target_heading = wrap_angle(state.heading - 20.0)
            log(f"[actions] manual RIGHT_CERCUS -> target heading {state.target_heading:.2f} for {state.robot_id}")
        elif channel in (CHANNEL_BOTH_CERCI, CHANNEL_BOTH_ANTENNAE):
            state.target_heading = wrap_angle(state.heading)
            log(f"[actions] manual forward -> lock target heading {state.target_heading:.2f} for {state.robot_id}")


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

                if state.x < WORLD_MIN_X:
                    state.x = WORLD_MIN_X
                    state.vx = abs(state.vx)
                elif state.x > WORLD_MAX_X:
                    state.x = WORLD_MAX_X
                    state.vx = -abs(state.vx)

                if state.y < WORLD_MIN_Y:
                    state.y = WORLD_MIN_Y
                    state.vy = abs(state.vy)
                elif state.y > WORLD_MAX_Y:
                    state.y = WORLD_MAX_Y
                    state.vy = -abs(state.vy)

                speed = math.hypot(state.vx, state.vy)
                state.moving = speed > 2.0

                if speed > 1.0:
                    state.heading = wrap_angle(math.degrees(math.atan2(state.vy, state.vx)))

        time.sleep(dt)


def controller_loop() -> None:
    dt = 1.0 / max(1.0, CONTROL_LOOP_HZ)

    while running:
        with state_lock:
            for state in robot_states.values():
                state.history.add_yaw(heading_to_yaw(state.heading))

                if state.target_heading is None:
                    continue

                if state.history.is_signal_running():
                    log("[controller] INFO: Not controlling, a signal is still executing")
                    continue

                now = time.time()
                if now - state.history.last_signal_time() < MIN_TIME_BETWEEN_SIGNALS:
                    continue

                current_heading = state.heading
                yaw = heading_to_yaw(current_heading)
                target_yaw = heading_to_yaw(state.target_heading)
                angle_diff = yaw_diff_to_target(yaw, target_yaw)
                if angle_diff is None:
                    continue

                log(
                    f"[controller] {time.strftime('%H:%M:%S')} INFO: current heading: {current_heading:.1f} | "
                    f"target (actions): {state.target_heading:.6f} | current yaw: {yaw:.1f} | "
                    f"target yaw: {target_yaw:.6f} | angle_diff: {angle_diff:.6f} for marker {state.marker_id}"
                )

                if abs(angle_diff) > ANGLE_TOLERANCE_BROAD:
                    channel = CHANNEL_RIGHT_ANTENNA if angle_diff < 0 else CHANNEL_LEFT_ANTENNA
                    apply_signal_to_robot(state, channel, 0.8333333333, 40, 500, source="controller")
                elif abs(angle_diff) > ANGLE_TOLERANCE:
                    channel = CHANNEL_RIGHT_CERCUS if angle_diff < 0 else CHANNEL_LEFT_CERCUS
                    apply_signal_to_robot(state, channel, 0.52, 40, 400, source="controller")
                else:
                    if KEEP_MOVING_ENABLED and not state.moving:
                        apply_signal_to_robot(state, CHANNEL_BOTH_CERCI, 0.52, 40, 500, source="controller")
                        log("[controller] INFO: [movement] BOTH_CERCI nudge ampFactor=0.52 (movement=False, step-on-message)")

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
    threading.Thread(target=controller_loop, daemon=True).start()
    threading.Thread(target=publish_sim_state_loop, args=(client,), daemon=True).start()

    while running:
        time.sleep(0.5)

    client.loop_stop()
    client.disconnect()
    log("stopped")


if __name__ == "__main__":
    main()