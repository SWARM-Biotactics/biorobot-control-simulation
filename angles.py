from __future__ import annotations


def normalize_deg(x: float) -> float:
    return float(x) % 360.0


def heading_to_yaw(heading: float) -> float:
    h = heading % 360.0
    if h > 180.0:
        return h - 360.0
    return h


def yaw_diff_to_target(current_yaw: float, target_yaw: float) -> float | None:
    for val in (current_yaw, target_yaw):
        if not -180.0 <= val <= 180.0:
            return None

    diff = target_yaw - current_yaw
    while diff <= -180.0:
        diff += 360.0
    while diff > 180.0:
        diff -= 360.0
    return diff