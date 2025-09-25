import os

# RabbitMQ 配置
RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", "127.0.0.1")
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", 5672))

# Celery Worker 配置
WORKER_USERNAME = os.environ.get("WORKER_USERNAME", "worker")
WORKER_PASSWORD = os.environ.get("WORKER_PASSWORD", "worker")

# MySQL 配置
MYSQL_HOST = os.environ.get("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", 3306))
MYSQL_USERNAME = os.environ.get("MYSQL_USERNAME", "root")
MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD", "1234")

# BigQuery 配置
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-project-id")
