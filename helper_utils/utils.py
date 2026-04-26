from __future__ import annotations

import os
import time


def load_session_id_config() -> str:
    return os.getenv("SESSION_ID", "local-sim-session")


def write_log(session_id, marker_id, message) -> None:
    print(f"[session={session_id} marker={marker_id}] {message}", flush=True)


def write_position(session_id, marker_id, animal_id, backpack_id, x, y, yaw) -> None:
    return None


def write_signal(
    session_id,
    marker_id,
    animal_id,
    backpack_id,
    channel_name,
    channel,
    waveform,
    frequency,
    amplitude,
    duration,
) -> None:
    return None