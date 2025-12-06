FROM python:3.11-slim
WORKDIR /app
COPY services/user_service/requirements.txt /app/requirements.txt
RUN apt-get update && apt-get install -y gcc libpq-dev && rm -rf /var/lib/apt/lists/*
RUN pip install --upgrade pip
RUN pip install -r /app/requirements.txt
COPY services/user_service /app/services/user_service
COPY services/common_lib/protos_generated /app/services/common_lib/protos_generated
ENV PYTHONPATH=/app
EXPOSE 50058
CMD ["python", "services/user_service/app.py"]
