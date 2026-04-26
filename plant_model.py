from __future__ import annotations

import math
import random
import time


CHANNEL_BOTH_CERCI = 0
CHANNEL_LEFT_ANTENNA = 1
CHANNEL_RIGHT_ANTENNA = 2
CHANNEL_LEFT_CERCUS = 3
CHANNEL_RIGHT_CERCUS = 4
CHANNEL_BOTH_ANTENNAE = 5


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def wrap_angle(deg: float) -> float:
    return deg % 360.0


def apply_signal_to_robot(
    state,
    channel: int,
    amplitude_factor: float,
    frequency: float,
    duration_ms: float,
) -> None:
    now = time.time()
    amp = clamp(float(amplitude_factor), 0.182, 1.0)
    duration_s = max(0.05, float(duration_ms) / 1000.0)

    if channel in (CHANNEL_BOTH_CERCI, CHANNEL_BOTH_ANTENNAE):
        state.intent.forward_until = max(state.intent.forward_until, now + duration_s)
        return

    if channel == CHANNEL_LEFT_ANTENNA:
        state.intent.turn_dir = 1
        state.intent.turn_until = max(state.intent.turn_until, now + duration_s)
        return

    if channel == CHANNEL_RIGHT_ANTENNA:
        state.intent.turn_dir = -1
        state.intent.turn_until = max(state.intent.turn_until, now + duration_s)
        return

    if channel == CHANNEL_LEFT_CERCUS:
        state.intent.turn_dir = 1
        state.intent.turn_until = max(state.intent.turn_until, now + duration_s * 0.7)
        return

    if channel == CHANNEL_RIGHT_CERCUS:
        state.intent.turn_dir = -1
        state.intent.turn_until = max(state.intent.turn_until, now + duration_s * 0.7)
        return


def step_robot(
    state,
    dt: float,
    bounds: tuple[float, float, float, float],
    default_speed: float,
    turn_rate_deg: float,
    drag: float,
    jitter: float,
) -> bool:
    now = time.time()
    min_x, max_x, min_y, max_y = bounds

    if now < state.intent.turn_until and state.intent.turn_dir != 0:
        state.heading = wrap_angle(state.heading + state.intent.turn_dir * turn_rate_deg * dt)
    elif now >= state.intent.turn_until:
        state.intent.turn_dir = 0

    if now < state.intent.forward_until:
        rad = math.radians(state.heading)
        accel = default_speed
        state.vx += math.cos(rad) * accel * dt
        state.vy += math.sin(rad) * accel * dt

    state.vx *= drag
    state.vy *= drag

    state.vx += random.uniform(-jitter, jitter) * dt
    state.vy += random.uniform(-jitter, jitter) * dt

    state.x += state.vx * dt
    state.y += state.vy * dt

    hit_wall = False

    if state.x < min_x:
        state.x = min_x
        state.vx = abs(state.vx) * 0.3
        hit_wall = True
    elif state.x > max_x:
        state.x = max_x
        state.vx = -abs(state.vx) * 0.3
        hit_wall = True

    if state.y < min_y:
        state.y = min_y
        state.vy = abs(state.vy) * 0.3
        hit_wall = True
    elif state.y > max_y:
        state.y = max_y
        state.vy = -abs(state.vy) * 0.3
        hit_wall = True

    if hit_wall:
        state.intent.forward_until = 0.0
        state.intent.turn_until = 0.0
        state.intent.turn_dir = 0

    speed = math.hypot(state.vx, state.vy)
    state.moving = speed > 2.0

    if speed > 1.0 and state.intent.turn_dir == 0:
        velocity_heading = wrap_angle(math.degrees(math.atan2(state.vy, state.vx)))
        state.heading = wrap_angle(0.85 * state.heading + 0.15 * velocity_heading)

    return hit_wall