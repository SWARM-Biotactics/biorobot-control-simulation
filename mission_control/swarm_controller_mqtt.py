import argparse
from mission_control.mqtt_client import MqttClient
from collections import deque
import math
import time
import json
import helper_utils.utils
import threading
from datetime import datetime, timezone

# --- Args ---
ap = argparse.ArgumentParser("Biorobot Controller (MQTT-config only)")
ap.add_argument("--biorobot-uuid", required=True, help="UUID of the biorobot to control (used to derive ip/port)")
ap.add_argument("--bootstrap-mqtt-host", default="10.0.100.150")
ap.add_argument("--bootstrap-mqtt-port", type=int, default=1883)
ap.add_argument("--bootstrap-config-topic", default="config")
ap.add_argument("--bootstrap-timeout", type=float, default=5.0)
args = ap.parse_args()

client = MqttClient(
    biorobot_id=args.biorobot_uuid,
    broker_host=args.bootstrap_mqtt_host,
    broker_port=args.bootstrap_mqtt_port,
)

offset = -170

# Targets (progress) + config
targets = deque()
base_targets = []
target_radius = 600
targets_loop = False

targets_lock = threading.Lock()
config_ready = threading.Event()

# session scope (resets only when session_id changes)
session_id = helper_utils.utils.load_session_id_config()

# --- waypoint topics ---
WAYPOINTS_STATS_TOPIC = f"waypoints/biorobot/{args.biorobot_uuid}"     # non-retained lap stats
WAYPOINTS_STATE_TOPIC = f"waypoints/state/{args.biorobot_uuid}"        # retained lap state
WAYPOINTS_PROGRESS_TOPIC = f"waypoints/progress/{args.biorobot_uuid}"  # retained remaining-targets progress

# --- lap state (retained via WAYPOINTS_STATE_TOPIC) ---
lap_index = 1
lap_start_epoch = None  # float | None

# Retained readiness (we DO wait on startup now to be robust)
state_ready = threading.Event()
progress_ready = threading.Event()

# Cache retained payloads if they arrive before config
_cached_state_payload = None
_cached_progress_payload = None


def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def within_threshold(x1, y1, x2, y2, threshold):
    distance = math.hypot(x2 - x1, y2 - y1)
    return distance < threshold


def bearing_from_object_to_point(obj_x, obj_y, pt_x, pt_y):
    dx = pt_x - obj_x
    dy = pt_y - obj_y   # +y is south, -y is north
    angle_rad = math.atan2(dx, -dy)
    angle_deg = math.degrees(angle_rad)
    bearing = (angle_deg + offset) % 360
    return bearing


def _normalize_targets(cfg_targets):
    if not isinstance(cfg_targets, list) or len(cfg_targets) == 0:
        raise ValueError("Config missing/invalid 'targets' (must be a non-empty list)")
    normalized = []
    for t in cfg_targets:
        if not isinstance(t, dict) or "x" not in t or "y" not in t:
            continue
        normalized.append({"x": float(t["x"]), "y": float(t["y"])})
    if len(normalized) == 0:
        raise ValueError("Config 'targets' list contained no valid {x,y} entries")
    return normalized


def _normalize_remaining_targets(rem):
    if not isinstance(rem, list):
        return []
    out = []
    for t in rem:
        if isinstance(t, dict) and "x" in t and "y" in t:
            try:
                out.append({"x": float(t["x"]), "y": float(t["y"])})
            except Exception:
                pass
    return out


# ---------------- Retained lap state ----------------

def _publish_waypoints_state_locked() -> None:
    payload = {
        "session_id": session_id,
        "biorobot_uuid": args.biorobot_uuid,
        "targets_loop": bool(targets_loop),
        "lap_index": int(lap_index),
        "lap_start_epoch": None if lap_start_epoch is None else float(lap_start_epoch),
        "lap_start_time": None if lap_start_epoch is None else datetime.fromtimestamp(lap_start_epoch, tz=timezone.utc).isoformat(),
        "updated_at": iso_utc_now(),
    }
    try:
        client._client.publish(WAYPOINTS_STATE_TOPIC, json.dumps(payload), qos=1, retain=True)
    except Exception as e:
        print(f"Could not publish retained waypoint state to {WAYPOINTS_STATE_TOPIC}: {e}")


def _reset_lap_state_locked(publish_reset: bool = True) -> None:
    global lap_index, lap_start_epoch
    lap_index = 1
    lap_start_epoch = None
    if publish_reset:
        _publish_waypoints_state_locked()


def _ensure_lap_started_on_first_position_locked() -> None:
    global lap_start_epoch
    if not targets_loop:
        return
    if lap_start_epoch is None:
        lap_start_epoch = time.time()
        _publish_waypoints_state_locked()


def _apply_retained_state_locked(payload: dict) -> None:
    """
    Apply retained lap state. If session mismatch -> reset + publish reset.
    IMPORTANT: do NOT publish on normal load.
    """
    global lap_index, lap_start_epoch

    recv_session = payload.get("session_id", None)
    if recv_session and str(recv_session) != str(session_id):
        _reset_lap_state_locked(publish_reset=True)
        state_ready.set()
        return

    li = payload.get("lap_index", None)
    ls = payload.get("lap_start_epoch", None)

    if li is not None:
        try:
            lap_index = int(li)
        except Exception:
            lap_index = 1

    if ls is not None:
        try:
            lap_start_epoch = float(ls)
        except Exception:
            lap_start_epoch = None

    # Do NOT start lap here (starts on first position)
    state_ready.set()


def _publish_lap_stats(start_epoch: float, end_epoch: float, lap_no: int) -> None:
    duration_min = (end_epoch - start_epoch) / 60.0
    payload = {
        "session_id": session_id,
        "biorobot_uuid": args.biorobot_uuid,
        "lap": int(lap_no),
        "start_time": datetime.fromtimestamp(start_epoch, tz=timezone.utc).isoformat(),
        "end_time": datetime.fromtimestamp(end_epoch, tz=timezone.utc).isoformat(),
        "duration_minutes": float(round(duration_min, 4)),
        "published_at": iso_utc_now(),
    }
    try:
        client._client.publish(WAYPOINTS_STATS_TOPIC, json.dumps(payload), qos=1, retain=False)
    except Exception as e:
        print(f"Could not publish lap stats to {WAYPOINTS_STATS_TOPIC}: {e}")


# ---------------- Retained progress ----------------

def _publish_waypoints_progress_locked() -> None:
    payload = {
        "session_id": session_id,
        "biorobot_uuid": args.biorobot_uuid,
        "remaining_targets": list(targets),
        "remaining_count": int(len(targets)),
        "updated_at": iso_utc_now(),
    }
    try:
        client._client.publish(WAYPOINTS_PROGRESS_TOPIC, json.dumps(payload), qos=1, retain=True)
    except Exception as e:
        print(f"Could not publish retained waypoint progress to {WAYPOINTS_PROGRESS_TOPIC}: {e}")


def _reset_progress_locked(publish_reset: bool = True) -> None:
    targets.clear()
    targets.extend(list(base_targets))
    if publish_reset:
        _publish_waypoints_progress_locked()
    progress_ready.set()


def _apply_retained_progress_locked(payload: dict) -> None:
    """
    Apply retained progress. If session mismatch -> reset + publish reset.
    IMPORTANT: do NOT publish on normal load.
    """
    recv_session = payload.get("session_id", None)
    if recv_session and str(recv_session) != str(session_id):
        _reset_progress_locked(publish_reset=True)
        return

    rem = _normalize_remaining_targets(payload.get("remaining_targets", []))
    targets.clear()
    if len(rem) > 0:
        targets.extend(rem)
    else:
        # empty retained progress means "done"; if looping, restart
        if targets_loop and len(base_targets) > 0:
            targets.extend(list(base_targets))

    progress_ready.set()


def apply_config(cfg: dict, *, initial: bool) -> None:
    """
    Config sets base_targets/params.
    IMPORTANT: Do NOT initialize/publish default progress/state here.
    Startup init is handled in main() after waiting for retained state/progress.
    """
    global base_targets, target_radius, targets_loop, _cached_progress_payload, _cached_state_payload

    cfg_targets = cfg.get("targets", [])
    cfg_radius = cfg.get("target_radius", None)
    cfg_loop = cfg.get("targets_loop", None)

    normalized = _normalize_targets(cfg_targets)

    with targets_lock:
        base_targets = normalized
        if cfg_radius is not None:
            target_radius = float(cfg_radius)
        if cfg_loop is not None:
            targets_loop = bool(cfg_loop)

        # If retained payloads arrived before config, apply them now (no publish).
        if initial and _cached_progress_payload is not None:
            _apply_retained_progress_locked(_cached_progress_payload)
            _cached_progress_payload = None

        if initial and _cached_state_payload is not None:
            _apply_retained_state_locked(_cached_state_payload)
            _cached_state_payload = None

        # If config updates mid-run: do not reset progress.
        # But if looping enabled and targets empty, restart and publish (progress changed).
        if (not initial) and targets_loop and len(targets) == 0 and len(base_targets) > 0:
            targets.extend(list(base_targets))
            _publish_waypoints_progress_locked()

    try:
        client.publish_log(
            f"[swarm_controller] Config updated: n_base={len(base_targets)} "
            f"target_radius={target_radius} targets_loop={targets_loop} "
            f"{'(initialized)' if initial else '(progress preserved)'}"
        )
    except Exception:
        pass


def publish_go_to_heading(payload):
    global lap_index, lap_start_epoch

    x, y = float(payload.get("x")), float(payload.get("y"))

    with targets_lock:
        _ensure_lap_started_on_first_position_locked()

        if len(targets) == 0:
            return

        target = targets[0]
        radius = target_radius
        loop_enabled = targets_loop
        base = list(base_targets)

    if not within_threshold(x, y, target["x"], target["y"], radius):
        bearing_with_offset = bearing_from_object_to_point(x, y, target["x"], target["y"])
        try:
            client.publish_heading_action_execute(
                biorobot_uuid=args.biorobot_uuid,
                target_heading=bearing_with_offset
            )
        except Exception as e:
            print(f"Could not send heading: {e}")
        return

    # waypoint reached
    publish_lap = False
    lap_payload = None  # (start_epoch, end_epoch, lap_no)

    with targets_lock:
        if len(targets) == 0:
            return

        reached_target = targets.popleft()

        if len(targets) == 0 and loop_enabled and len(base) > 0:
            now = time.time()

            if lap_start_epoch is not None:
                publish_lap = True
                lap_payload = (lap_start_epoch, now, lap_index)

            # reset for next lap
            targets.clear()
            targets.extend(base)

            lap_index += 1
            lap_start_epoch = now

            _publish_waypoints_state_locked()

            try:
                client.publish_log(
                    f"Reached last target: {reached_target}. targets_loop=true -> restarting from first target."
                )
            except Exception:
                pass

        elif len(targets) == 0:
            try:
                client.publish_log(f"Successfully reached last target: {reached_target}")
            except Exception:
                pass
        else:
            try:
                client.publish_log(f"Moving to next target: {targets[0]}")
            except Exception:
                pass

        # IMPORTANT: publish progress ONLY when waypoint changes
        _publish_waypoints_progress_locked()

    if publish_lap and lap_payload is not None:
        start_e, end_e, lap_no = lap_payload
        _publish_lap_stats(start_e, end_e, lap_no)


def on_message(mqtt_client, userdata, msg):
    global _cached_progress_payload, _cached_state_payload

    try:
        payload = json.loads(msg.payload.decode("utf-8"))

        if msg.topic == args.bootstrap_config_topic:
            is_initial = not config_ready.is_set()
            apply_config(payload, initial=is_initial)
            config_ready.set()
            return

        if msg.topic == WAYPOINTS_STATE_TOPIC:
            with targets_lock:
                if not config_ready.is_set():
                    _cached_state_payload = payload
                    return
                _apply_retained_state_locked(payload)
            return

        if msg.topic == WAYPOINTS_PROGRESS_TOPIC:
            with targets_lock:
                if not config_ready.is_set():
                    _cached_progress_payload = payload
                    return
                _apply_retained_progress_locked(payload)
            return

        publish_go_to_heading(payload)

    except Exception as e:
        print(f"Exception during on_message: {e}")
        _ = repr(msg.payload)


def main():
    client.start()

    while not client._connected:
        print("Waiting for MQTT connection ...")
        time.sleep(0.2)

    mqtt = client._client
    mqtt.on_message = on_message

    # Subscribe to retained topics first
    cfg_topic = args.bootstrap_config_topic
    mqtt.subscribe(WAYPOINTS_STATE_TOPIC)
    print(f"Subscribed to waypoint state topic '{WAYPOINTS_STATE_TOPIC}'")

    mqtt.subscribe(WAYPOINTS_PROGRESS_TOPIC)
    print(f"Subscribed to waypoint progress topic '{WAYPOINTS_PROGRESS_TOPIC}'")

    mqtt.subscribe(cfg_topic)
    print(f"Subscribed to config topic '{cfg_topic}'")

    # Wait for config (required)
    if not config_ready.wait(timeout=float(args.bootstrap_timeout)):
        print(f"ERROR: Did not receive retained config on '{cfg_topic}' within {args.bootstrap_timeout}s")
        client.stop()
        return

    # Now wait for retained progress/state; if missing, initialize ONCE and publish defaults.
    # This prevents overwriting valid retained messages on restart.
    progress_ready.wait(timeout=float(args.bootstrap_timeout))
    state_ready.wait(timeout=float(args.bootstrap_timeout))

    with targets_lock:
        if not progress_ready.is_set():
            # no retained progress -> initialize from config and publish once
            targets.clear()
            targets.extend(list(base_targets))
            _publish_waypoints_progress_locked()
            progress_ready.set()

        if not state_ready.is_set():
            # no retained state -> publish a minimal container for this session (does not start lap)
            _reset_lap_state_locked(publish_reset=True)
            state_ready.set()

    # Now subscribe to positions (control loop)
    position_topic = f"sensors/{args.biorobot_uuid}/position/+"
    mqtt.subscribe(position_topic)
    print(f"Subscribed to position topic '{position_topic}'")

    print("Listening for messages. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
        client.stop()


if __name__ == "__main__":
    main()
