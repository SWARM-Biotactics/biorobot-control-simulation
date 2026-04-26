import time
import numpy as np
from collections import deque

class BiorobotHistory:
    """
    Tracks yaw (-180..180] and stimulation (signal log) history for a biorobot.
    All alignment calculations are performed in yaw-space.

    Stored entries:
      - self.yaw_history: deque of {"time": float, "yaw": float}
      - self.signal_log : deque of {"time", "channel", "frequency", "amplitude", "duration_ms"}
    """

    def __init__(self, maxlen: int = 100):
        self.yaw_history = deque(maxlen=maxlen)
        self.signal_log  = deque(maxlen=maxlen)
        self.total_signal_count = 0
        self.angle_correction_signal_count = 0

    # === Yaw tracking ===

    def add_yaw(self, yaw: float):
        """Append a new yaw measurement with timestamp (yaw in [-180, 180])."""
        self.yaw_history.append({"time": time.time(), "yaw": float(yaw)})

    def recent_yaws(self, window: float):
        """Return yaw samples within the last `window` seconds."""
        cutoff = time.time() - window
        return [e for e in self.yaw_history if e["time"] >= cutoff]

    # === Signal tracking ===

    def add_signal(self, channel, frequency, amplitude, duration):
        """Append a new stimulation event to the signal log (duration in ms)."""
        self.signal_log.append({
            "time": time.time(),
            "channel": channel,
            "frequency": frequency,
            "amplitude": amplitude,
            "duration": duration  # ms
        })
        self.total_signal_count += 1
        if channel in (1, 2):
            self.angle_correction_signal_count += 1

    def get_total_signals(self) -> int:
        return self.total_signal_count

    def get_total_corrections(self) -> int:
        return self.angle_correction_signal_count

    def last_signal_time(self, channel_filter=None) -> float:
        """Timestamp of the most recent signal (optionally filter by channel)."""
        for entry in reversed(self.signal_log):
            if channel_filter is None or entry["channel"] == channel_filter:
                return entry["time"]
        return 0.0

    def get_last_signal(self, channel: int, window: float = 120.0):
        """Most recent signal for given channel within `window` seconds, else None."""
        cutoff = time.time() - window
        for entry in reversed(self.signal_log):
            if entry["time"] < cutoff:
                break
            if entry["channel"] == channel:
                return entry
        return None

    def last_cerci_signal(self, window: float = 120.0):
        """Most recent LEFT/RIGHT cercus (channels 3 or 4) within window, else None."""
        cutoff = time.time() - window
        for entry in reversed(self.signal_log):
            if entry["time"] < cutoff:
                break
            if entry["channel"] in (3, 4):
                return entry
        return None

    def last_individual_cerci_time(self, window: float = 120.0) -> float | None:
        """Timestamp of most recent LEFT/RIGHT cercus within window, else None."""
        s = self.last_cerci_signal(window)
        return s["time"] if s else None

    def is_signal_running(self, channel_filter=None) -> bool:
        """True if the latest (optionally channel-filtered) signal is still executing."""
        now = time.time()
        for entry in reversed(self.signal_log):
            if channel_filter is None or entry["channel"] == channel_filter:
                end_time = entry["time"] + (entry["duration"] / 1000.0)
                return now < end_time
        return False

    # === Alignment improvement evaluation (yaw-space) ===

    def direction_improved_by_cerci(
        self,
        target_yaw: float,
        window: float = 120.0,
        before_window: float = 2.0,
        after_window: float = 2.0
    ) -> bool:
        """
        Determine if the most recent cercus stimulation improved alignment to `target_yaw`.
        """
        signal = self.last_cerci_signal(window)
        if not signal:
            return False

        stim_time = signal["time"]
        used_channel = signal["channel"]

        def yaw_err(yaw: float, target: float) -> float:
            return (target - yaw + 180.0) % 360.0 - 180.0

        before_diffs, after_diffs = [], []
        for e in self.yaw_history:
            t = e["time"]
            if stim_time - before_window <= t < stim_time:
                before_diffs.append(yaw_err(e["yaw"], target_yaw))
            elif stim_time < t <= stim_time + after_window:
                after_diffs.append(yaw_err(e["yaw"], target_yaw))

        if not before_diffs or not after_diffs:
            return False

        initial_diff = float(np.mean(before_diffs))
        new_diff     = float(np.mean(after_diffs))

        if used_channel == 3:  # LEFT
            return abs(new_diff) < abs(initial_diff) and new_diff < initial_diff
        elif used_channel == 4:  # RIGHT
            return abs(new_diff) < abs(initial_diff) and new_diff > initial_diff
        return False

    def direction_improved_by_channel(
        self,
        channel: int,
        target_yaw: float,
        window: float = 120.0,
        return_diffs: bool = False
    ):
        """
        Generic improvement check in yaw-space for any `channel`.
        Returns bool or (bool, before_diff, after_diff).
        """
        cutoff = time.time() - window
        signal = None
        for entry in reversed(self.signal_log):
            if entry["time"] < cutoff:
                break
            if entry["channel"] == channel:
                signal = entry
                break
        if not signal:
            return (False, None, None) if return_diffs else False

        stim_start = signal["time"]
        stim_end   = stim_start + (signal["duration"] / 1000.0)

        def yaw_err(yaw: float, target: float) -> float:
            return (target - yaw + 180.0) % 360.0 - 180.0

        before = next((e for e in reversed(self.yaw_history) if e["time"] < stim_start), None)
        after  = next((e for e in self.yaw_history if e["time"] > stim_end), None)

        if before is None or after is None:
            return (False, None, None) if return_diffs else False

        before_diff = yaw_err(before["yaw"], target_yaw)
        after_diff  = yaw_err(after["yaw"],  target_yaw)
        improved    = abs(after_diff) < abs(before_diff)

        return (improved, before_diff, after_diff) if return_diffs else improved
