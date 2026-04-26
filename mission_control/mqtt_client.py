from __future__ import annotations

import json
import time
import uuid

import paho.mqtt.client as mqtt


class MqttClient:
    def __init__(self, biorobot_id: str, broker_host: str, broker_port: int) -> None:
        self.biorobot_id = biorobot_id
        self.broker_host = broker_host
        self.broker_port = broker_port
        self._connected = False
        self._client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"sim-{biorobot_id}-{uuid.uuid4().hex[:8]}",
        )
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        self._connected = True

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties=None):
        self._connected = False

    def start(self) -> None:
        self._client.connect(self.broker_host, self.broker_port, keepalive=60)
        self._client.loop_start()

    def stop(self) -> None:
        try:
            self._client.loop_stop()
        finally:
            self._client.disconnect()

    def _ensure_connected(self, timeout: float = 5.0) -> None:
        t0 = time.time()
        while not self._connected and (time.time() - t0) < timeout:
            time.sleep(0.05)
        if not self._connected:
            raise RuntimeError("MQTT not connected")

    def publish_log(self, message: str) -> None:
        payload = {
            "source": "mission_control_sim",
            "message": str(message),
            "timestamp": int(time.time() * 1000),
        }
        self._client.publish("logs/mission_control", json.dumps(payload), qos=1, retain=False)

    def publish_action_execute(
        self,
        biorobot_uuid: str,
        channel: int,
        amplitude_factor: float,
        frequency: float,
        duration_ms: float,
    ) -> None:
        topic = f"actions/biorobot/{biorobot_uuid}/execute"
        payload = {
            "timestamp": int(time.time() * 1000),
            "action": "Signal",
            "channel": int(channel),
            "amplitudeFactor": float(amplitude_factor),
            "frequency": float(frequency),
            "durationMs": float(duration_ms),
        }
        self._client.publish(topic, json.dumps(payload), qos=1, retain=False)

    def publish_heading_action_execute(self, biorobot_uuid: str, target_heading: float) -> None:
        topic = f"actions/biorobot/{biorobot_uuid}/execute"
        payload = {
            "timestamp": int(time.time() * 1000),
            "action": "GoToHeading",
            "target_heading": float(target_heading),
        }
        self._client.publish(topic, json.dumps(payload), qos=1, retain=False)