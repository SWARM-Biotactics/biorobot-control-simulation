from __future__ import annotations

import json
import math
import random
from dataclasses import dataclass, field
from typing import Any


@dataclass
class RobotConfig:
    uuid: str
    marker_id: str
    robot_id: str
    ip: str = ""
    port: int = 3130
    animal_id: str = ""


@dataclass
class RuntimeConfig:
    robots: dict[str, RobotConfig] = field(default_factory=dict)
    heading_offset: float = 0.0
    target_heading: float | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    @staticmethod
    def from_payload(payload: bytes | str) -> "RuntimeConfig":
        if isinstance(payload, bytes):
            payload = payload.decode("utf-8", errors="replace")

        data = json.loads(payload)

        robots: dict[str, RobotConfig] = {}
        heading_offset = float(data.get("heading_offset", 0.0))
        target_heading = data.get("target_heading", None)
        if target_heading is not None:
            try:
                target_heading = float(target_heading) % 360.0
            except Exception:
                target_heading = None

        biorobots = data.get("biorobots", []) or []

        for idx, item in enumerate(biorobots):
            uuid = str(item.get("uuid") or item.get("id") or "")
            marker_id = str(item.get("marker_id") or item.get("markerId") or item.get("marker") or "")
            ip = str(item.get("ip_address") or item.get("ip") or "")
            port = int(item.get("port") or 3130)
            animal_id = str(item.get("animal_id") or "")
            if not uuid:
                continue

            robot_name = f"robot{marker_id}" if marker_id else f"robot{idx+1}"

            robots[uuid] = RobotConfig(
                uuid=uuid,
                marker_id=marker_id,
                robot_id=robot_name,
                ip=ip,
                port=port,
                animal_id=animal_id,
            )

        return RuntimeConfig(
            robots=robots,
            heading_offset=heading_offset,
            target_heading=target_heading,
            raw=data,
        )


class ConfigStore:
    def __init__(self) -> None:
        self.runtime = RuntimeConfig()

    def update_from_payload(self, payload: bytes | str) -> RuntimeConfig:
        self.runtime = RuntimeConfig.from_payload(payload)
        return self.runtime

    def has_robots(self) -> bool:
        return bool(self.runtime.robots)

    def robot_uuid_by_marker(self, marker_id: str) -> str | None:
        for uuid, cfg in self.runtime.robots.items():
            if cfg.marker_id == str(marker_id):
                return uuid
        return None


def seeded_pose(index: int, total: int, center_x: float = 5000.0, center_y: float = 5000.0, radius: float = 1200.0) -> tuple[float, float, float]:
    total = max(1, total)
    angle = (2.0 * math.pi * index) / total
    x = center_x + radius * math.cos(angle)
    y = center_y + radius * math.sin(angle)
    heading = math.degrees(angle + math.pi / 2.0) % 360.0
    return x, y, heading


def random_jitter(scale: float = 5.0) -> float:
    return random.uniform(-scale, scale)