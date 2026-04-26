from __future__ import annotations

import time
from collections import deque


class BiorobotHistory:
    def __init__(self, maxlen: int = 200):
        self.yaw_history = deque(maxlen=maxlen)
        self.signal_log = deque(maxlen=maxlen)
        self.total_signal_count = 0
        self.angle_correction_signal_count = 0

    def add_yaw(self, yaw: float) -> None:
        self.yaw_history.append({"time": time.time(), "yaw": float(yaw)})

    def add_signal(self, channel: int, frequency: float, amplitude: float, duration_ms: float) -> None:
        self.signal_log.append(
            {
                "time": time.time(),
                "channel": int(channel),
                "frequency": float(frequency),
                "amplitude": float(amplitude),
                "duration": float(duration_ms),
            }
        )
        self.total_signal_count += 1
        if channel in (1, 2):
            self.angle_correction_signal_count += 1

    def get_total_signals(self) -> int:
        return self.total_signal_count

    def get_total_corrections(self) -> int:
        return self.angle_correction_signal_count

    def last_signal_time(self, channel_filter=None) -> float:
        for entry in reversed(self.signal_log):
            if channel_filter is None or entry["channel"] == channel_filter:
                return entry["time"]
        return 0.0

    def get_last_signal(self, channel: int, window: float = 120.0):
        cutoff = time.time() - window
        for entry in reversed(self.signal_log):
            if entry["time"] < cutoff:
                break
            if entry["channel"] == channel:
                return entry
        return None

    def is_signal_running(self, channel_filter=None) -> bool:
        now = time.time()
        for entry in reversed(self.signal_log):
            if channel_filter is None or entry["channel"] == channel_filter:
                end_time = entry["time"] + entry["duration"] / 1000.0
                return now < end_time
        return False