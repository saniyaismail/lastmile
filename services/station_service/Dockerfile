FROM python:3.11-slim
WORKDIR /app
COPY services/station_service/requirements.txt /app/requirements.txt
RUN apt-get update && apt-get install -y gcc libpq-dev gdal-bin proj-bin && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt
# copy the service code
COPY services/station_service /app/services/station_service
# copy generated protos
COPY services/common_lib/protos_generated /app/services/common_lib/protos_generated
ENV PYTHONPATH=/app
EXPOSE 50051
CMD ["python", "services/station_service/app.py"]
