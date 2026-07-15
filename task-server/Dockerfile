FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY tasks.py .
# Default to worker; docker-compose overrides `command` for celery-beat.
CMD ["celery", "-A", "tasks", "worker", "--loglevel=info"]
