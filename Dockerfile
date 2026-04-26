FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sim_control.py .
COPY config_runtime.py .
COPY mqtt_topics.py .
COPY controller_history.py .
COPY angles.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "sim_control.py"]