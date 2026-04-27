FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sim_control.py .
COPY config_runtime.py .
COPY plant_model.py .
COPY angles.py .
COPY mqtt_topics.py .

COPY helper_utils ./helper_utils
COPY mission_control ./mission_control
COPY entrypoints ./entrypoints

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["python", "entrypoints/run_plant.py"]