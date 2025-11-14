FROM python:3.11-slim
WORKDIR /app
COPY services/location_service/requirements.txt /app/requirements.txt
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt
COPY services/location_service /app/services/location_service
COPY services/common_lib/protos_generated /app/services/common_lib/protos_generated
ENV PYTHONPATH=/app
EXPOSE 50053
CMD ["python", "services/location_service/app.py"]