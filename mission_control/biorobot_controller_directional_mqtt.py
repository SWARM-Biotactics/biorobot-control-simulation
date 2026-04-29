#!/usr/bin/env python3
"""
Biorobot Controller (MQTT-config only, telemetry via MQTT)

- Bootstraps config from an MQTT broker (retained message)
- Listens for live config updates
- Subscribes to this bot's telemetry topics:
    sensors/{biorobot_uuid}/compass/{source}
    sensors/{biorobot_uuid}/position/{source}
    sensors/{biorobot_uuid}/movement/{source}
- Runs the 'directional' approach loop using telemetry queue
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import queue
import socket
import sys
import threading
import time
from typing import Any, Dict, Optional, Tuple


from mission_control.mqtt_client import MqttClient
from mission_control.biorobot_history import BiorobotHistory
from helper_utils.angles import heading_to_yaw, yaw_diff_to_target
from helper_utils.utils import (
    load_session_id_config,
    write_log,
    write_position,
    write_signal,
)

# ---------- Logging ------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="[controller] %(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("controller")

# ---------- Globals / State ----------------------------------------------------

# MQ-fed telemetry queue (compass/position/movement events)
telemetry_q: "queue.Queue[tuple]" = queue.Queue()

# Dynamic system_config access (filled from MQTT config only)
system_config: Dict[str, Any] = {}
CONFIG_LOCK = threading.Lock()

def _apply_system_config(new_cfg: dict) -> None:
    """Atomically replace system_config dict from MQTT."""
    global system_config
    with CONFIG_LOCK:
        system_config = dict(new_cfg)
    log.info("system_config updated from MQTT")

def get_cfg(path: str, default=None):
    """Read nested config key like 'foo.bar.baz' with a default."""
    with CONFIG_LOCK:
        node = system_config
        for part in path.split("."):
            if isinstance(node, dict) and part in node:
                node = node[part]
            else:
                return default
        return node

# --- Target heading override (set by actions topic) ---
TARGET_HEADING_OVERRIDE_LOCK = threading.Lock()
TARGET_HEADING_OVERRIDE: Optional[float] = None

def set_target_heading_override(val: float) -> None:
    """Set actions-driven target heading override (0..360)."""
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            raise ValueError("non-finite")
        f = f % 360.0
    except Exception:
        log.warning(f"[actions] rejecting target_heading={val!r}")
        return
    global TARGET_HEADING_OVERRIDE
    with TARGET_HEADING_OVERRIDE_LOCK:
        TARGET_HEADING_OVERRIDE = f
    #log.info(f"[actions] target_heading override set to {TARGET_HEADING_OVERRIDE:.6f}")


def get_current_target_heading() -> Optional[float]:
    """Return current target heading in [0,360) — actions override wins over config."""
    with TARGET_HEADING_OVERRIDE_LOCK:
        if TARGET_HEADING_OVERRIDE is not None:
            return TARGET_HEADING_OVERRIDE
    th = get_cfg("target_heading", None)
    if th is None:
        return None
    try:
        f = float(th)
        if math.isnan(f) or math.isinf(f):
            return None
        return f % 360.0
    except Exception:
        return None

# --- Args ---
ap = argparse.ArgumentParser("Biorobot Controller (MQTT-config only)")
ap.add_argument("--biorobot-uuid", required=True, help="UUID of the biorobot to control (used to derive ip/port)")
ap.add_argument("--bootstrap-mqtt-host", default="localhost")
ap.add_argument("--bootstrap-mqtt-port", type=int, default=1883)
ap.add_argument("--bootstrap-config-topic", default="config")
ap.add_argument("--bootstrap-timeout", type=float, default=5.0)
args = ap.parse_args()

# --- Resolved after config arrives ---
mqtt: Optional[MqttClient] = None
session_id = load_session_id_config()
biorobot_uuid: str = args.biorobot_uuid
marker_id: str = "unknown"
animal_id: str = "unknown"
biorobot_ip: Optional[str] = None
biorobot_port: Optional[int] = None
backpack_id: Optional[str] = None

# UDP socket (signals to backpack)
send_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

history = BiorobotHistory()

def find_bot_by_uuid(uuid_str: str) -> Optional[dict]:
    bots = get_cfg("biorobots", []) or []
    for b in bots:
        if b.get("uuid") == uuid_str:
            return b
    return None

def use_weak_start() -> bool:
    return bool(get_cfg("biorobot_controller.start_with_weak_signals", False)) and (
        history.get_total_corrections() < int(get_cfg("biorobot_controller.weak_start_signals_count", 10))
    )

# ---------- Stall config (read live from config topic) -------------------------

def cfg_stall_seconds() -> float:
    """Seconds without motion before we consider the bot 'stalled'."""
    return float(get_cfg("biorobot_controller.stall_seconds", 5.0))

def cfg_min_move_px() -> float:
    """Minimum pixel displacement that counts as motion."""
    return float(get_cfg("biorobot_controller.min_move_px", 50.0))

def cfg_stall_cooldown_seconds() -> float:
    """Cooldown between consecutive BOTH_CERCI nudges (legacy; unused in movement-driven mode)."""
    return float(get_cfg("biorobot_controller.stall_cooldown_seconds", 2.0))

# Runtime motion state (legacy timestamps retained; movement feed drives nudging)
LAST_MOTION_TS = time.time()
LAST_POS: Optional[Tuple[float, float]] = None
_last_stall_nudge_ts = 0.0  # legacy; unused for movement-driven nudging

# Movement-driven nudge state
MOVING: Optional[bool] = None
NUDGE_AMP_FACTOR: float = 0.30  # start; +0.11 per consecutive moving=False msg (cap 0.75)

def _mark_motion(x: Optional[float] = None, y: Optional[float] = None) -> None:
    """Record motion and reset legacy stall fields; movement feed controls nudges."""
    global LAST_MOTION_TS, _last_stall_nudge_ts, LAST_POS
    LAST_MOTION_TS = time.time()
    _last_stall_nudge_ts = 0.0
    if x is not None and y is not None:
        LAST_POS = (x, y)

def _handle_movement_update(moving_bool: bool) -> None:
    """
    Movement-driven BOTH_CERCI nudging:
      - moving=True  -> reset ramp to 0.30 (no nudge)
      - moving=False -> send nudge now at current factor; then factor += 0.11 (cap 0.75)
    """
    global MOVING, NUDGE_AMP_FACTOR
    MOVING = bool(moving_bool)

    if MOVING:
        if NUDGE_AMP_FACTOR != 0.30:
            log.info("[movement] moving=True -> reset BOTH_CERCI ramp to 0.30")
        NUDGE_AMP_FACTOR = 0.30
        _mark_motion()
        return

    # moving == False: nudge immediately if allowed
    if not bool(get_cfg("biorobot_controller.keep_moving_enabled", False)):
        return
    if history.is_signal_running():
        return

    amp = NUDGE_AMP_FACTOR
    send_command(0, 40, amp, 500)  # CHANNEL_BOTH_CERCI = 0
    log.info(f"[movement] BOTH_CERCI nudge ampFactor={amp:.2f} (movement=False, step-on-message)")
    # increment for next consecutive false
    NUDGE_AMP_FACTOR = min(NUDGE_AMP_FACTOR + 0.11, 0.75)

# ---------- I/O to backpack (MQTT) ---------------------------------------------

def send_command(channel: int, frequency: float, amplitude_factor: float, duration: float) -> None:
    send_time = time.time()
    try:
        frequency = round(max(10.0, min(frequency, 100.0)))
        amplitude_factor = max(0.182, min(amplitude_factor, 1.0))
        amplitude = max(1000.0, min(amplitude_factor * 6600.0, 6600.0))
        duration = round(max(10.0, min(duration, 10000.0)))

        channel_name = {
            1: "LEFT_ANTENNA",
            2: "RIGHT_ANTENNA",
            3: "LEFT_CERCUS",
            4: "RIGHT_CERCUS",
            5: "BOTH_ANTENNAE",
            0: "BOTH_CERCI",
        }.get(channel, "UNKNOWN_CHANNEL")

        formatted_time = time.strftime("%H:%M:%S", time.localtime(send_time))
        log.info(f"{formatted_time} - Sending signal {channel_name}: {frequency}, {amplitude}, {duration} to backpack {backpack_id} with marker {marker_id}")
        write_log(session_id, marker_id, f"{formatted_time} - Sending signal {channel_name}: {frequency}, {amplitude}, {duration} to backpack {backpack_id} with marker {marker_id}")
        write_signal(session_id, marker_id, animal_id, backpack_id, channel_name, channel, "Square", frequency, amplitude, duration)
        if get_cfg("biorobot_controller.suppress_sending_signals_to_backpack", False):
            msg = "NOT sending mqtt msg for signal to biorobot"
            log.info(msg); write_log(session_id, marker_id, msg)
        else:
            gap = float(get_cfg("biorobot_controller.time_between_signalconfig_messages", 0.0))
            assert biorobot_ip and biorobot_port is not None, "Backpack IP/port not set"

            # publish action execute
            if mqtt:
                mqtt.publish_action_execute(
                    biorobot_uuid,
                    channel,
                    amplitude_factor,
                    frequency,
                    duration
                )

            history.add_signal(channel, frequency, amplitude, duration)

        try:
            if mqtt:
                mqtt.publish_log(f"{formatted_time} - Signal {channel_name}: {frequency}, {amplitude}, {duration} ")
        except Exception:
            pass

    except Exception:
        write_log(session_id, marker_id, "could not send signal message")
        log.exception("could not send signal message")

# ---------- Control logic ------------------------------------------------------

CHANNEL_LEFT_ANTENNA=1; CHANNEL_RIGHT_ANTENNA=2; CHANNEL_LEFT_CERCUS=3; CHANNEL_RIGHT_CERCUS=4; CHANNEL_BOTH_ANTENNAE=5; CHANNEL_BOTH_CERCI=0

def approach_directional() -> None:
    log.info("starting approach_directional loop ...")
    global LAST_MOTION_TS, LAST_POS, _last_stall_nudge_ts
    while True:
        processed = False
        try:
            while True:
                try:
                    ev = telemetry_q.get_nowait()
                except queue.Empty:
                    break

                processed = True
                kind = ev[0]

                if kind == "position":
                    _, x, y, heading_opt = ev
                    position_received_mqtt(x, y, heading_opt)

                    # legacy motion (for logs); movement feed controls nudging
                    try:
                        if LAST_POS is None:
                            LAST_POS = (x, y)
                        else:
                            dx = x - LAST_POS[0]
                            dy = y - LAST_POS[1]
                            dist = (dx*dx + dy*dy) ** 0.5
                            if dist >= cfg_min_move_px():
                                _mark_motion(x, y)
                    except Exception:
                        pass

                elif kind == "compass":
                    # tuple can be ("compass", event_type, heading_val) from older code,
                    # or ("compass", event_type, heading_val, source) after this patch.
                    if len(ev) == 4:
                        _, event_type, heading_val, source = ev
                    else:
                        _, event_type, heading_val = ev
                        source = "backpack"  # sensible default

                    if event_type == "changing":
                        # Treat compass-changing as motion
                        _mark_motion()
                        compass_update_received(heading_val)

                    elif event_type == "stabilized":
                        compass_stop_received(heading_val)

                elif kind == "movement":
                    _, moving_bool = ev
                    _handle_movement_update(moving_bool)

        except Exception:
            log.exception("exception when receiving messages")
            #write_log(session_id, marker_id, "exception when receiving messages (see logs)")
        if not processed:
            time.sleep(0.01)  # small backoff when idle


def position_received_mqtt(x: float, y: float, yaw: Optional[float]) -> None:
    # write_position (yaw 0.0 if None)
    write_position(session_id, marker_id, animal_id, backpack_id, x, y, yaw if yaw is not None else 0.0)
    print("")

def compass_update_received(current_heading: float) -> None:
    try:
        yaw = heading_to_yaw(current_heading)
        #log.info(f"heading: {current_heading} | yaw: {yaw}")
    except (ValueError, TypeError):
        log.info("compass_update_received aborted"); return
    history.add_yaw(yaw)

def compass_stop_received(current_heading: float) -> None:
    """Handle a stabilized compass event (heading in degrees)."""
    try:
        yaw = heading_to_yaw(current_heading)
        #log.info(f"heading: {current_heading} | yaw: {yaw}")
    except (ValueError, TypeError):
        log.info("compass_stop_received aborted")
        return

    # Track latest yaw in history
    history.add_yaw(yaw)

    # Determine target heading (actions override wins over config)
    target_heading = get_current_target_heading()
    source = "config"
    with TARGET_HEADING_OVERRIDE_LOCK:
        if TARGET_HEADING_OVERRIDE is not None:
            source = "actions"

    if target_heading is None:
        log.info("Skipping control — missing target direction.")
        return

    if history.is_signal_running():
        log.info("Not controlling, a signal is still executing")
        return

    # Throttle control frequency
    now = time.time()
    last_time = history.last_signal_time(channel_filter=None)
    min_gap = int(get_cfg("biorobot_controller.min_time_between_signals", 0))
    if (now - last_time) <= min_gap:
        log.info("Not controlling, throttled")
        return

    # Compute target yaw + policy thresholds
    try:
        target_yaw = heading_to_yaw(target_heading)
    except (TypeError, ValueError):
        log.info("Invalid target_heading encountered.")
        return

    ANGLE_TOLERANCE = int(get_cfg("biorobot_controller.angle_tolerance", 30))
    ANGLE_TOLERANCE_BROAD = int(get_cfg("biorobot_controller.angle_tolerance_broad", 60))

    angle_diff = yaw_diff_to_target(yaw, target_yaw)

    log_message = (
        f"current heading: {current_heading} | target ({source}): {target_heading} | "
        f"current yaw: {yaw} | target yaw: {target_yaw} | angle_diff: {angle_diff} "
        f"for marker {marker_id}"
    )
    log.info(log_message)
    mqtt.publish_log(log_message)
    #write_log(session_id, marker_id, log_message)

    # Control decision
    if abs(angle_diff) > ANGLE_TOLERANCE_BROAD:
        channel = CHANNEL_RIGHT_ANTENNA if angle_diff < 0 else CHANNEL_LEFT_ANTENNA
        amplitude_factor = adjust_amplitude(history, channel, target_yaw, 300.0) or 0.3
        send_command(channel, 40, amplitude_factor, 500)

    elif abs(angle_diff) > ANGLE_TOLERANCE:
        channel = CHANNEL_RIGHT_CERCUS if angle_diff < 0 else CHANNEL_LEFT_CERCUS
        amplitude_factor = adjust_amplitude(history, channel, target_yaw, 300.0) or 0.3
        #send_command(channel, 45, amplitude_factor, 500)

def adjust_amplitude(history: BiorobotHistory, channel: int, target_yaw: float, window: float = 120.0) -> Optional[float]:
    last_signal = history.get_last_signal(channel, window=window)
    if not last_signal:
        log.info(f"No recent signal for channel {channel}"); return None
    last_amplitude = last_signal["amplitude"]
    improvement, before_diff, after_diff = history.direction_improved_by_channel(
        channel=channel, target_yaw=target_yaw, window=window, return_diffs=True
    )
    overshot = False if (before_diff is None or after_diff is None) else (before_diff * after_diff) < 0
    if improvement and not overshot:
        new_abs = min(last_amplitude * 1.0, 5500)
    elif improvement and overshot:
        new_abs = max(last_amplitude * 0.95, 2000)
    else:
        new_abs = min(last_amplitude * 1.1, 5500)
    return min(new_abs / 6600.0, 1.0)

# ---------- MAIN ---------------------------------------------------------------

CONTROL_START_TIME = time.time()

def main() -> None:
    global mqtt, marker_id, animal_id, biorobot_ip, biorobot_port, backpack_id

    log.info("Starting biorobot controller (MQTT-only config)...")
    #write_log(session_id, marker_id, "controller starting (MQTT-only)")

    # Bootstrap: connect + wait retained config
    temp_id = f"{biorobot_uuid}"
    mqtt = MqttClient(biorobot_id=temp_id,
                      broker_host=args.bootstrap_mqtt_host,
                      broker_port=args.bootstrap_mqtt_port)
    mqtt.start()

    t0 = time.time()
    while not mqtt._connected and (time.time() - t0) < args.bootstrap_timeout:
        time.sleep(0.05)

    if not mqtt._connected:
        log.error("MQTT not connected during bootstrap window — cannot proceed")
        sys.exit(1)

    # Fetch retained config once
    first = queue.Queue(maxsize=1)
    def _bootstrap_on_msg(client, userdata, msg):
        if first.empty():
            first.put_nowait(msg.payload)

    mqtt._client.on_message = _bootstrap_on_msg
    rc, mid = mqtt._client.subscribe(args.bootstrap_config_topic, qos=1)
    if rc != 0:
        log.error(f"subscribe('{args.bootstrap_config_topic}') failed rc={rc}")
        sys.exit(1)

    log.info(f"[config] bootstrapping from {args.bootstrap_config_topic} on {args.bootstrap_mqtt_host}:{args.bootstrap_mqtt_port}")

    try:
        payload = first.get(timeout=args.bootstrap_timeout)
        cfg = json.loads(payload.decode("utf-8"))
        _apply_system_config(cfg)
        log.info("[config] applied initial config")
    except queue.Empty:
        log.error("No initial config received — cannot proceed")
        sys.exit(1)
    except Exception as e:
        log.error(f"Failed to parse initial config: {e}")
        sys.exit(1)

    # Optional broker switch
    cfg_mqtt_ip = get_cfg("mqtt.ip", args.bootstrap_mqtt_host)
    cfg_mqtt_port = int(get_cfg("mqtt.port", args.bootstrap_mqtt_port))
    if (cfg_mqtt_ip, cfg_mqtt_port) != (args.bootstrap_mqtt_host, args.bootstrap_mqtt_port):
        try: mqtt.publish_log("Reconnecting MQTT publisher to configured broker")
        except Exception: pass
        try: mqtt.stop()
        except Exception: pass
        mqtt = MqttClient(biorobot_id=temp_id, broker_host=cfg_mqtt_ip, broker_port=cfg_mqtt_port)
        mqtt.start(); mqtt._ensure_connected()

    # Subscribe for live config updates (set default on_message)
    def _on_config_update(client, userdata, msg):
        try:
            new_cfg = json.loads(msg.payload.decode("utf-8"))

            # If override is active and the config also contains a target_heading, just log that we'll ignore it.
            with TARGET_HEADING_OVERRIDE_LOCK:
                if TARGET_HEADING_OVERRIDE is not None:
                    if "target_heading" in new_cfg:
                        log.info("[config] target_heading present in config update but ignored due to actions override")

            _apply_system_config(new_cfg)
            try:
                if mqtt: mqtt.publish_log("Applied config update")
            except Exception: pass
        except Exception as e:
            log.error(f"[config] failed to apply update: {e}")

    cfg_topic = get_cfg("topics.config", args.bootstrap_config_topic)
    mqtt._client.on_message = _on_config_update
    mqtt._client.subscribe(cfg_topic, qos=1)
    log.info(f"[config] subscribed to {cfg_topic} for dynamic updates")

    # Resolve this controller's biorobot by UUID
    bot = find_bot_by_uuid(biorobot_uuid)
    if not bot:
        log.error(f"biorobot UUID {biorobot_uuid} not found in config")
        sys.exit(1)

    biorobot_ip = bot.get("ip_address")
    biorobot_port = int(bot.get("port"))
    marker_id = bot.get("marker_id", "unknown")
    animal_id = bot.get("animal_id", "unknown")
    backpack_id = f"{biorobot_ip}:{biorobot_port}"

    # --- Telemetry subscriptions (use message_callback_add so config handler remains) ---
    compass_topic   = f"sensors/{biorobot_uuid}/compass/+"
    position_topic  = f"sensors/{biorobot_uuid}/position/+"
    movement_topic  = f"sensors/{biorobot_uuid}/movement/+"

    def _on_any_telemetry(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            log.warning(f"[telemetry] bad json on {msg.topic}")
            return

        # Compass: {"timestamp":..., "heading":..., "eventType":"changing|stabilized"}
        if msg.topic.startswith(f"sensors/{biorobot_uuid}/compass/"):
            try:
                ev = (payload.get("eventType") or "").lower()
                heading_raw = payload.get("heading", None)
                # Parse source from topic tail: sensors/{uuid}/compass/{source}
                src = msg.topic.rsplit("/", 1)[-1].lower()

                if heading_raw is None:
                    return
                try:
                    hv = float(heading_raw)
                except Exception:
                    return
                if math.isnan(hv) or math.isinf(hv):
                    return

                telemetry_q.put(("compass", ev, hv, src))
            except Exception as e:
                log.warning(f"[telemetry] compass parse error: {e}")

        # Position: {"timestamp":..., "x":..., "y":..., "eventType":..., "heading":...}
        # XXX currently long, lat messages are not supported
        elif msg.topic.startswith(f"sensors/{biorobot_uuid}/position/cameratracking"):
            try:
                x = float(payload.get("x"))
                y = float(payload.get("y"))
                heading_opt = payload.get("heading")  # may be None
                heading_val = float(heading_opt) if heading_opt is not None else None
                telemetry_q.put(("position", x, y, heading_val))
            except Exception as e:
                log.warning(f"[telemetry] position parse error: {e}")

        # Movement: {"timestamp":..., "moving": true|false} or {"movementState": 1|0}
        elif msg.topic.startswith(f"sensors/{biorobot_uuid}/movement/cameratracking"):
            try:
                moving_bool = bool(payload.get("moving"))
                telemetry_q.put(("movement", moving_bool))
            except Exception as e:
                log.warning(f"[telemetry] movement parse error: {e}")

    # Attach per-topic callbacks without overwriting the config on_message handler
    mqtt._client.message_callback_add(compass_topic, _on_any_telemetry)
    mqtt._client.message_callback_add(position_topic, _on_any_telemetry)
    mqtt._client.message_callback_add(movement_topic, _on_any_telemetry)
    mqtt._client.subscribe([(compass_topic, 1), (position_topic, 1), (movement_topic, 1)])
    log.info(f"[telemetry] subscribed to {compass_topic}, {position_topic}, {movement_topic}")

    # --- Actions subscription: set target_heading override from commands ---
    actions_topic = f"actions/biorobot/{biorobot_uuid}/execute"

    def _on_action(client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            log.warning(f"[actions] bad json on {msg.topic}")
            return

        action = (payload.get("action") or "").strip()
        if action == "GoToHeading":
            if "target_heading" not in payload:
                log.warning("[actions] GoToHeading missing 'target_heading'")
                return
            try:
                th = float(payload["target_heading"])
            except (TypeError, ValueError):
                log.warning(f"[actions] invalid target_heading: {payload.get('target_heading')!r}")
                return

            set_target_heading_override(th)
            try:
                if mqtt: mqtt.publish_log(f"[actions] GoToHeading received; override={th:.6f}")
            except Exception:
                pass

    # Attach without clobbering other handlers
    mqtt._client.message_callback_add(actions_topic, _on_action)
    mqtt._client.subscribe(actions_topic, qos=1)
    log.info(f"[actions] subscribed to {actions_topic}")

    log.info(f"using approach 'directional' for biorobot {biorobot_uuid} / {backpack_id} with marker {marker_id}")
    approach_directional()

if __name__ == "__main__":
    main()