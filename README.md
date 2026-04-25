# biorobot-control-simulation

Simulation-only replacement for the real biorobot control service.

## What it does

This container:

- subscribes to MQTT config on `config`
- subscribes to `actions/biorobot/+/execute`
- simulates robot state per configured biorobot
- publishes:
  - `sensors/<uuid>/position/cameratracking`
  - `sensors/<uuid>/compass/cameratracking`
  - `sensors/<uuid>/movement/cameratracking`
  - `robots/status`
  - `tracking/cameras/status`

## Action mapping

For `{"action":"Signal","channel":...}`:

- `channel=0` -> turn left
- `channel=1` -> move forward
- `channel=2` -> turn right

## Build

```bash
docker build -t real-time-camera-tracking-simulation:latest .