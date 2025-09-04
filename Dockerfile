# 使用 Ubuntu 22.04 作為基礎映像檔
FROM ubuntu:22.04

# 更新套件列表，並安裝 Python 3.10 以及 pip 和 curl (用於下載套件)
RUN apt-get update && \
    apt-get install -y python3.10 python3-pip curl 

# 安裝 uv
RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh \
 && uv --version

WORKDIR /app

# 先複製依賴檔，讓快取生效
COPY pyproject.toml uv.lock README.md ./

# 用 uv 安裝依賴（會建 .venv）
RUN uv sync

# 複製主要程式碼目錄
COPY data_ingestion/ ./data_ingestion/

# 複製環境變數檔案
COPY .env ./

# 建立 output 目錄
RUN mkdir -p output

# 設定語系環境和時區變數
ENV LC_ALL=C.UTF-8 LANG=C.UTF-8 TZ=Asia/Taipei

# 啟動容器後，預設執行 bash（開啟終端）
CMD ["/bin/bash"]
