FROM python:3.11-slim
WORKDIR /app
COPY services/driver_service/requirements.txt /app/requirements.txt
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt
COPY services/driver_service /app/services/driver_service
COPY services/common_lib/protos_generated /app/services/common_lib/protos_generated
ENV PYTHONPATH=/app
EXPOSE 50052
CMD ["python", "services/driver_service/app.py"]