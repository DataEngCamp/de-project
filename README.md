# de-project
2025 Data Engineering Course Project


# 資料夾結構



# 建立虛擬環境並安裝依賴（同步）
uv sync

# 建立一個 network 讓各服務能溝通
docker network create my_network

# 指令

```
# 啟動服務
docker compose -f docker-compose-broker.yml up -d

# 停止並移除服務
docker compose -f docker-compose-broker.yml down

＃ 查看服務 logs
docker logs -f rabbitmq
docker logs -f flower

# producer 發送任務
uv run data_ingestion/producer.py

# 啟動 worker
uv run celery -A data_ingestion.worker worker --loglevel=info --hostname=worker1%h
uv run celery -A data_ingestion.worker worker --loglevel=info --hostname=worker2%h


```