FROM python:3.11-slim
WORKDIR /app
COPY services/notification_service/requirements.txt /app/requirements.txt
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*
RUN pip install -r requirements.txt
COPY services/notification_service /app/services/notification_service
COPY services/common_lib/protos_generated /app/services/common_lib/protos_generated
ENV PYTHONPATH=/app
EXPOSE 50056
CMD ["python", "services/notification_service/app.py"]