from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RobotConfig:
    uuid: str
    marker_id: str
    robot_id: str
    animal_id: str = ""
    ip: str = ""
    port: int = 3130


@dataclass
class RuntimeConfig:
    robots: dict[str, RobotConfig] = field(default_factory=dict)
    raw: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_payload(payload: bytes | str) -> "RuntimeConfig":
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8", errors="replace")

        data = json.loads(payload)
        robots: dict[str, RobotConfig] = {}

        for idx, item in enumerate(data.get("biorobots", []) or []):
            uuid = str(item.get("uuid") or item.get("id") or "").strip()
            if not uuid:
                continue

            marker_id = str(
                item.get("marker_id") or item.get("markerId") or item.get("marker") or ""
            ).strip()
            robot_id = str(item.get("robot_id") or "").strip()
            if not robot_id:
                robot_id = f"robot{marker_id}" if marker_id else f"robot{idx + 1}"

            robots[uuid] = RobotConfig(
                uuid=uuid,
                marker_id=marker_id,
                robot_id=robot_id,
                animal_id=str(item.get("animal_id") or "").strip(),
                ip=str(item.get("ip_address") or item.get("ip") or "").strip(),
                port=int(item.get("port") or 3130),
            )

        return RuntimeConfig(robots=robots, raw=data)


class ConfigStore:
    def __init__(self) -> None:
        self.runtime = RuntimeConfig()

    def update_from_payload(self, payload: bytes | str) -> RuntimeConfig:
        self.runtime = RuntimeConfig.from_payload(payload)
        return self.runtime

    def has_robots(self) -> bool:
        return bool(self.runtime.robots)

    def robot_uuid_by_marker(self, marker_id: str) -> str | None:
        marker_id = str(marker_id)
        for uuid, cfg in self.runtime.robots.items():
            if cfg.marker_id == marker_id:
                return uuid
        return None


def seeded_pose(
    index: int,
    total: int,
    center_x: float = 5000.0,
    center_y: float = 5000.0,
    radius: float = 1200.0,
) -> tuple[float, float, float]:
    total = max(1, total)
    angle = (2.0 * math.pi * index) / total
    x = center_x + radius * math.cos(angle)
    y = center_y + radius * math.sin(angle)
    heading = math.degrees(angle + math.pi / 2.0) % 360.0
    return x, y, heading