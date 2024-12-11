FROM python:3.9-slim

WORKDIR /app

COPY . .

# Install dependencies and make scripts module discoverable
RUN pip install -e .

# Add PYTHONPATH to include the current directory
ENV PYTHONPATH=/app:${PYTHONPATH}

ENTRYPOINT ["python", "-m", "scripts.buenarda_worker"]