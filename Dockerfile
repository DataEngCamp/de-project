# 使用 Ubuntu 22.04 作為基礎映像檔
FROM ubuntu:22.04

# 更新套件列表，並安裝 Python 3.10、pip 和 netcat（用於連線檢查）
RUN apt-get update && \
    apt-get install -y python3.10 python3-pip netcat

# 安裝 uv (快速的 Python 套件管理工具)
RUN pip install --no-cache-dir uv

# 建立工作目錄 /app
RUN mkdir /app

# 將當前目錄（與 Dockerfile 同層）所有內容複製到容器的 /app 資料夾
COPY . /app/

# 設定容器的工作目錄為 /app，後續的指令都在這個目錄下執行
WORKDIR /app

# 使用 uv 安裝依賴（基於 pyproject.toml 和 uv.lock）
RUN uv sync

# 安裝專案為可編輯套件，讓 Python 能找到 data_ingestion 模組
# RUN uv pip install -e .

# 設定語系環境變數，避免 Python 編碼問題
ENV LC_ALL=C.UTF-8
ENV LANG=C.UTF-8

# 啟動容器後，預設執行 bash（開啟終端）
CMD ["/bin/bash"]
