
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY scripts/ ./scripts/
COPY buenarda_worker.py .

ENTRYPOINT ["python", "buenarda_worker.py"]