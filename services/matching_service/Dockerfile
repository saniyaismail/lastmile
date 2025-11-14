FROM python:3.11-slim
WORKDIR /app
COPY services/matching_service/requirements.txt /app/requirements.txt
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt
COPY services/matching_service /app/services/matching_service
COPY services/common_lib/protos_generated /app/services/common_lib/protos_generated
ENV PYTHONPATH=/app
EXPOSE 50054
CMD ["python", "services/matching_service/app.py"]