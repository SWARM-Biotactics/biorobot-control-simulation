# mqtt_client.py
from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from typing import Optional
from collections import deque 

from paho.mqtt import client as paho_mqtt
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes


def _now_millis() -> int:
    return int(time.time() * 1000)


def _expiry_props(seconds: Optional[int]) -> Optional[Properties]:
    if seconds is None or seconds <= 0:
        return None
    p = Properties(PacketTypes.PUBLISH)
    p.MessageExpiryInterval = int(seconds)
    return p


@dataclass
class MqttConfig:
    broker_host: str = "localhost"
    broker_port: int = 1883
    keepalive_s: int = 30
    client_id: Optional[str] = None
    # Defaults
    qos_reading: int = 1
    qos_log: int = 0
    expiry_position_s: int = 15
    expiry_compass_s: int = 10
    retain_reading: bool = False
    retain_log: bool = False


class MqttClient:
    """
    Minimal MQTT v5 publisher with simple auto-reconnect on publish.
    - No buffering
    - No backoff
    - `start()` never raises if broker is unreachable
    """

    def __init__(
        self,
        *,
        biorobot_id: str,
        broker_host: str = "localhost",
        broker_port: int = 1883,
        keepalive_s: int = 30,
        client_id: Optional[str] = None,
        source_id: Optional[str] = None,
        qos_reading: int = 1,
        qos_log: int = 0,
        expiry_position_s: int = 15,
        expiry_compass_s: int = 10,
        retain_reading: bool = False,
        retain_log: bool = False,
        print_fn=print,
    ) -> None:
        self.biorobot_id = biorobot_id
        self.source_id = source_id or biorobot_id
        self._print = print_fn

        self.cfg = MqttConfig(
            broker_host=broker_host,
            broker_port=broker_port,
            keepalive_s=keepalive_s,
            client_id=client_id,
            qos_reading=qos_reading,
            qos_log=qos_log,
            expiry_position_s=expiry_position_s,
            expiry_compass_s=expiry_compass_s,
            retain_reading=retain_reading,
            retain_log=retain_log,
        )

        cid = self.cfg.client_id or f"swarm-pub-{uuid.uuid4()}"
        self._client = paho_mqtt.Client(client_id=cid, protocol=paho_mqtt.MQTTv5)
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._connected = False

    # -------- Lifecycle --------
    def start(self) -> None:
        """
        Start background loop and try to connect *asynchronously*.
        This never raises if the broker is down.
        """
        try:
            self._client.loop_start()
        except Exception:
            # If loop_start fails (rare), we still keep running without MQTT.
            pass
        try:
            # connect_async doesn’t throw if broker is offline; it schedules a connect attempt.
            self._client.connect_async(self.cfg.broker_host, self.cfg.broker_port, self.cfg.keepalive_s)
            # Optional: a tiny kick so an immediate attempt happens.
            self._client.reconnect()
        except Exception as e:
            self._print(f"[mqtt] start(): connect attempt not established yet ({e})")

    def stop(self) -> None:
        try:
            self._client.loop_stop()
        except Exception:
            pass
        try:
            self._client.disconnect()
        except Exception:
            pass
        self._connected = False

    def __enter__(self) -> "MqttClient":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.stop()

    # -------- Callbacks --------
    def _on_connect(self, client, userdata, flags, reasonCode, properties=None):
        self._connected = True
        self._print(f"[mqtt] connected rc={reasonCode}")

    def _on_disconnect(self, client, userdata, reasonCode, properties=None):
        self._connected = False
        self._print(f"[mqtt] disconnected rc={reasonCode}")

    # -------- Internal helper --------
    def _ensure_connected(self) -> bool:
        """
        If not connected, attempt a quick reconnect once.
        Return True if connected, else False.
        """
        if self._connected:
            return True
        try:
            self._client.reconnect()
        except Exception:
            # As a fallback, schedule async connect (no raise).
            try:
                self._client.connect_async(self.cfg.broker_host, self.cfg.broker_port, self.cfg.keepalive_s)
            except Exception:
                pass
            return False
        # Connection will be confirmed via on_connect; assume not connected yet for this call.
        return self._connected

    # -------- Publish APIs --------
    def publish_position(
        self,
        *,
        sensor_id: str,
        longitude: float,
        latitude: float,
        heading: Optional[float] = None,
        event_type: str = "changing",
        timestamp_ms: Optional[int] = None,
        qos: Optional[int] = None,
        retain: Optional[bool] = None,
        expiry_seconds: Optional[int] = None,
    ) -> None:
        topic = f"sensors/{self.biorobot_id}/position/{sensor_id}"
        payload = {
            "timestamp": int(timestamp_ms) if timestamp_ms is not None else _now_millis(),
            "longitude": float(longitude),
            "latitude": float(latitude),
            "eventType": str(event_type),
        }
        if heading is not None:
            payload["heading"] = float(heading)

        if not self._ensure_connected():
            return  # drop silently if still offline

        try:
            self._client.publish(
                topic,
                json.dumps(payload, separators=(",", ":")),
                qos=self.cfg.qos_reading if qos is None else int(qos),
                retain=self.cfg.retain_reading if retain is None else bool(retain),
                properties=_expiry_props(self.cfg.expiry_position_s if expiry_seconds is None else int(expiry_seconds)),
            )
        except Exception:
            # still offline or network error — drop
            pass

    def publish_position_xy(
        self,
        *,
        sensor_id: str,
        x: float,
        y: float,
        heading: Optional[float] = None,
        event_type: str = "changing",
        timestamp_ms: Optional[int] = None,
        qos: Optional[int] = None,
        retain: Optional[bool] = None,
        expiry_seconds: Optional[int] = None,
    ) -> None:
        """
        Publish local arena coordinates (x,y) instead of WGS84.
        Topic: sensors/{biorobot_id}/position/{sensor_id}
        Payload: {timestamp, x, y, [heading], eventType}
        """
        topic = f"sensors/{self.biorobot_id}/position/{sensor_id}"
        payload = {
            "timestamp": int(timestamp_ms) if timestamp_ms is not None else _now_millis(),
            "x": float(x),
            "y": float(y),
            "eventType": str(event_type),
        }
        if heading is not None:
            payload["heading"] = float(heading)

        if not self._ensure_connected():
            return

        try:
            self._client.publish(
                topic,
                json.dumps(payload, separators=(",", ":")),
                qos=self.cfg.qos_reading if qos is None else int(qos),
                retain=self.cfg.retain_reading if retain is None else bool(retain),
                properties=_expiry_props(self.cfg.expiry_position_s if expiry_seconds is None else int(expiry_seconds)),
            )
        except Exception:
            pass

    def publish_compass(
        self,
        *,
        sensor_id: str,
        heading: float,
        event_type: str = "changing",
        timestamp_ms: Optional[int] = None,
        qos: Optional[int] = None,
        retain: Optional[bool] = None,
        expiry_seconds: Optional[int] = None,
    ) -> None:
        topic = f"sensors/{self.biorobot_id}/compass/{sensor_id}"
        payload = {
            "timestamp": int(timestamp_ms) if timestamp_ms is not None else _now_millis(),
            "heading": float(heading),
            "eventType": str(event_type),
        }

        if not self._ensure_connected():
            return

        try:
            self._client.publish(
                topic,
                json.dumps(payload, separators=(",", ":")),
                qos=self.cfg.qos_reading if qos is None else int(qos),
                retain=self.cfg.retain_reading if retain is None else bool(retain),
                properties=_expiry_props(self.cfg.expiry_compass_s if expiry_seconds is None else int(expiry_seconds)),
            )
        except Exception:
            pass

    def publish_log(
        self,
        message: str,
        *,
        timestamp_ms: Optional[int] = None,
        qos: Optional[int] = None,
        retain: Optional[bool] = None,
    ) -> None:
        topic = f"logs/{self.source_id}"
        payload = {
            "timestamp": int(timestamp_ms) if timestamp_ms is not None else _now_millis(),
            "message": str(message),
        }

        if not self._ensure_connected():
            return

        try:
            self._client.publish(
                topic,
                json.dumps(payload, separators=(",", ":")),
                qos=self.cfg.qos_log if qos is None else int(qos),
                retain=self.cfg.retain_log if retain is None else bool(retain),
                properties=None,
            )
        except Exception:
            pass

    def publish_signal_action_execute(
        self,
        *,
        biorobot_uuid: str,
        channel: int,
        amplitude_factor: float,
        frequency: int,
        duration_ms: int,
    ) -> None:
        """
        Publish the executed signal command for this biorobot to:
          actions/biorobot/{biorobot_uuid}/execute
        """
        if not self._ensure_connected():
            return
        try:
            topic = f"actions/biorobot/{biorobot_uuid}/execute"
            payload = {
                "timestamp": _now_millis(),
                "action": "Signal",
                "channel": int(channel),                  # 0..5
                "amplitudeFactor": float(amplitude_factor),  # 0.182..1.0
                "frequency": int(frequency),              # 10..100 Hz
                "durationMs": int(duration_ms),           # 10..10000 ms
            }
            self._client.publish(
                topic,
                json.dumps(payload, separators=(",", ":")),
                qos=1,
                retain=False,
            )
        except Exception as e:
            self._print(f"MQTT signal action publish failed: {e}")

    def publish_heading_action_execute(
        self,
        *,
        biorobot_uuid: str,
        target_heading: float
    ) -> None:
        if not self._ensure_connected():
            return
        try:
            topic = f"actions/biorobot/{biorobot_uuid}/execute"
            payload = {
                "action": "GoToHeading",
                "target_heading": float(target_heading)
            }
            self._client.publish(
                topic,
                json.dumps(payload, separators=(",", ":")),
                qos=1,
                retain=False
            )
        except Exception as e:
            print(f"MQTT action publish failed: {e}")

    def publish_action_execute(self, biorobot_uuid: str, channel: int, amplitude_factor: float, frequency: int, duration_ms: int):
        """
        Publish the executed signal command for this biorobot to:
          actions/biorobot/{biorobot_uuid}/execute
        """
        if not self._ensure_connected():
            return
        try:
            topic = f"actions/biorobot/{biorobot_uuid}/execute"
            payload = {
                "action": "Signal",
                "channel": int(channel),                # 0..5
                "amplitudeFactor": float(amplitude_factor),  # 0.182..1.0
                "frequency": int(frequency),            # 10..100 Hz
                "durationMs": int(duration_ms)          # 10..10000 ms
            }
            self._client.publish(
                topic,
                json.dumps(payload, separators=(",", ":")),
                qos=1,
                retain=False
            )
        except Exception as e:
            print(f"MQTT action publish failed: {e}")
        
    
    def publish_status(
        self,
        *,
        biorobot_uuid: str,
        status: dict,
        qos: Optional[int] = None,
        retain: Optional[bool] = None,
    ) -> None:
        topic = f"biorobot/{biorobot_uuid}/status"
        payload = {
            "biorobot_uuid": biorobot_uuid,
            "target_queue": status.get("target_queue"),
        }

        try:
            self._client.publish(
                topic,
                json.dumps(payload, separators=(",", ":")),
                qos=self.cfg.qos_log if qos is None else int(qos),
                retain=True if retain is None else bool(retain),
                properties=None,
            )
        except Exception:
            pass