from celery import Celery
import multiprocessing

from data_ingestion.config import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    WORKER_USERNAME,
    WORKER_PASSWORD,
)

app = Celery(
    main="worker",
    # 載入以下路徑模組註冊的 Celery 任務
    include=[
        "data_ingestion.tasks",
        "data_ingestion.tasks_crawler_hahow_course",
        "data_ingestion.tasks_crawler_hahow_article",
        "data_ingestion.tasks_crawler_hahow",
    ],
    # 指定 broker 為 rabbitmq
    # pyamqp://worker:worker@127.0.0.1:5672/
    broker=f"pyamqp://{WORKER_USERNAME}:{WORKER_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/",
)

print(f"CPU 核心數: {multiprocessing.cpu_count()}")
print("🚀 Celery Worker 已啟動，等待接收任務...")
