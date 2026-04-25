from __future__ import annotations


def sensor_position_topic(robot_uuid: str) -> str:
    return f"sensors/{robot_uuid}/position/cameratracking"


def sensor_compass_topic(robot_uuid: str) -> str:
    return f"sensors/{robot_uuid}/compass/cameratracking"


def sensor_movement_topic(robot_uuid: str) -> str:
    return f"sensors/{robot_uuid}/movement/cameratracking"


def action_execute_topic(robot_uuid: str) -> str:
    return f"actions/biorobot/{robot_uuid}/execute"


ROBOTS_STATUS_TOPIC = "robots/status"
CAMERA_STATUS_TOPIC = "tracking/cameras/status"
CONFIG_TOPIC_DEFAULT = "config"