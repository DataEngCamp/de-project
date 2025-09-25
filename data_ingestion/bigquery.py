"""
BigQuery 操作模組
參考 mysql.py 的結構，提供 BigQuery 的資料庫操作功能
"""
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField, LoadJobConfig, WriteDisposition
import os
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from data_ingestion.config import GCP_PROJECT_ID as PROJECT_ID

# 設置日誌
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# BigQuery 配置
DATASET_ID = "hahow_analytics"

def get_bigquery_client():
    """建立 BigQuery 客戶端"""
    return bigquery.Client(project=PROJECT_ID)

def create_dataset_if_not_exists(dataset_id: str = DATASET_ID):
    """建立 BigQuery Dataset（如果不存在）"""
    client = get_bigquery_client()
    dataset_ref = client.dataset(dataset_id)
    
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_id} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # 或您偏好的區域
        dataset = client.create_dataset(dataset)
        logger.info(f"Created dataset {dataset_id}")

# ========== Table Schema 定義 ==========

def hahow_course_bq_schema():
    """定義 hahow_course 表的 BigQuery schema"""
    schema = [
        bigquery.SchemaField(name="id", field_type="STRING"),
        bigquery.SchemaField(name="category", field_type="STRING"),
        bigquery.SchemaField(name="uniquename", field_type="STRING"),
        bigquery.SchemaField(name="title", field_type="STRING"),
        bigquery.SchemaField(name="status", field_type="STRING"),
        bigquery.SchemaField(name="link", field_type="STRING"),
        bigquery.SchemaField(name="price", field_type="NUMERIC"),
        bigquery.SchemaField(name="preordered_price", field_type="NUMERIC"),
        bigquery.SchemaField(name="average_rating", field_type="FLOAT"),
        bigquery.SchemaField(name="num_rating", field_type="INTEGER"),
        bigquery.SchemaField(name="owner_name", field_type="STRING"),
        bigquery.SchemaField(name="sold_num", field_type="INTEGER"),
        bigquery.SchemaField(name="bookmark_count", field_type="INTEGER"),
        bigquery.SchemaField(name="meta_description", field_type="STRING"),
        bigquery.SchemaField(name="cover_image", field_type="STRING"),
        bigquery.SchemaField(name="incubate_time", field_type="TIMESTAMP"),
        bigquery.SchemaField(name="publish_time", field_type="TIMESTAMP"),
        bigquery.SchemaField(name="video_length", field_type="INTEGER"),
        bigquery.SchemaField(name="uploaded_at", field_type="TIMESTAMP"),
    ]
    return schema

def hahow_course_sales_bq_schema():
    """定義 hahow_course_sales 表的 BigQuery schema"""
    schema = [
        bigquery.SchemaField(name="id", field_type="INTEGER"),
        bigquery.SchemaField(name="course_id", field_type="STRING"),
        bigquery.SchemaField(name="price", field_type="NUMERIC"),
        bigquery.SchemaField(name="sold_num", field_type="INTEGER"),
        bigquery.SchemaField(name="captured_at", field_type="TIMESTAMP"),
        bigquery.SchemaField(name="uploaded_at", field_type="TIMESTAMP"),
    ]
    return schema

def hahow_article_bq_schema():
    """定義 hahow_article 表的 BigQuery schema"""
    schema = [
        bigquery.SchemaField(name="id", field_type="STRING"),
        bigquery.SchemaField(name="category", field_type="STRING"),
        bigquery.SchemaField(name="type", field_type="STRING"),
        bigquery.SchemaField(name="title", field_type="STRING"),
        bigquery.SchemaField(name="group_title", field_type="STRING"),
        bigquery.SchemaField(name="group_uniquename", field_type="STRING"),
        bigquery.SchemaField(name="subgroup_title", field_type="STRING"),
        bigquery.SchemaField(name="subgroup_uniquename", field_type="STRING"),
        bigquery.SchemaField(name="link", field_type="STRING"),
        bigquery.SchemaField(name="tags", field_type="STRING"),
        bigquery.SchemaField(name="creator_name", field_type="STRING"),
        bigquery.SchemaField(name="view_count", field_type="INTEGER"),
        bigquery.SchemaField(name="clap_total", field_type="INTEGER"),
        bigquery.SchemaField(name="preview_description", field_type="STRING"),
        bigquery.SchemaField(name="cover_image", field_type="STRING"),
        bigquery.SchemaField(name="created_at", field_type="TIMESTAMP"),
        bigquery.SchemaField(name="updated_at", field_type="TIMESTAMP"),
        bigquery.SchemaField(name="publish_at", field_type="TIMESTAMP"),
        bigquery.SchemaField(name="uploaded_at", field_type="TIMESTAMP"),
    ]
    return schema

# ========== 基礎操作函數 ==========

def create_table(table_name: str, schema: List[SchemaField], dataset_id: str = DATASET_ID):
    """
    建立 BigQuery 表
    
    Args:
        table_name: 表名
        schema: BigQuery schema
        dataset_id: Dataset ID
    """
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    # 確保 Dataset 存在
    create_dataset_if_not_exists(dataset_id)
    
    table = bigquery.Table(table_id, schema=schema)
    
    try:
        table = client.create_table(table)
        print(f"✅ 建立 BigQuery 表 '{table_name}' 成功")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"ℹ️  BigQuery 表 '{table_name}' 已存在")
        else:
            print(f"❌ 建立 BigQuery 表 '{table_name}' 失敗: {e}")
            raise

def upload_data_to_bigquery(table_name: str, df: pd.DataFrame, dataset_id: str = DATASET_ID, mode: str = "replace"):
    """
    上傳 DataFrame 到 BigQuery（類似 mysql.py 的 upload_data_to_mysql）
    
    Args:
        table_name: 表名
        df: 要上傳的 DataFrame
        dataset_id: Dataset ID
        mode: 寫入模式 ("replace", "append")
    """
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    # 確保 Dataset 存在
    create_dataset_if_not_exists(dataset_id)
    
    # 設定寫入模式
    if mode == "replace":
        write_disposition = WriteDisposition.WRITE_TRUNCATE
    elif mode == "append":
        write_disposition = WriteDisposition.WRITE_APPEND
    else:
        write_disposition = WriteDisposition.WRITE_EMPTY
    
    # 配置載入工作
    job_config = LoadJobConfig(
        write_disposition=write_disposition,
        autodetect=True,  # 自動偵測 schema
    )
    
    try:
        # 執行載入
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # 等待完成
        
        print(f"✅ 資料已上傳到 BigQuery 表 '{table_name}'，共 {len(df)} 筆記錄")
        
    except Exception as e:
        print(f"❌ 上傳資料到 BigQuery 表 '{table_name}' 失敗: {e}")
        raise

def upload_data_to_bigquery_insert(table_name: str, data: List[Dict], dataset_id: str = DATASET_ID):
    """
    使用 insert_rows_json 上傳資料到 BigQuery（類似 mysql.py 的 upload_data_to_mysql_insert）
    
    Args:
        table_name: 表名
        data: 要插入的資料列表
        dataset_id: Dataset ID
    """
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    # 確保 Dataset 存在
    create_dataset_if_not_exists(dataset_id)
    
    try:
        # 獲取表引用
        table = client.get_table(table_id)
        
        # 直接插入資料
        errors = client.insert_rows_json(table, data)
        
        if errors:
            print(f"❌ 插入資料到 BigQuery 表 '{table_name}' 時發生錯誤: {errors}")
            raise Exception(f"BigQuery 插入失敗: {errors}")
        
        print(f"✅ INSERT 完成，處理 {len(data)} 筆記錄到 BigQuery 表 '{table_name}'")
        
    except Exception as e:
        print(f"❌ 插入資料到 BigQuery 表 '{table_name}' 失敗: {e}")
        raise

def create_view(view_name: str, view_sql: str, dataset_id: str = DATASET_ID):
    """
    在 BigQuery 中建立或替換 View（類似 mysql.py 的 create_view）
    
    Args:
        view_name: View 的名稱
        view_sql: 建立 View 的 SQL 語句
        dataset_id: Dataset ID
    """
    client = get_bigquery_client()
    view_id = f"{PROJECT_ID}.{dataset_id}.{view_name}"
    
    # 確保 Dataset 存在
    create_dataset_if_not_exists(dataset_id)
    
    view = bigquery.Table(view_id)
    view.view_query = view_sql
    
    try:
        # 嘗試更新現有 view
        client.update_table(view, ["view_query"])
        print(f"✅ 更新 BigQuery View '{view_name}' 成功")
    except Exception:
        # 如果不存在則建立新的
        try:
            view = client.create_table(view)
            print(f"✅ 建立 BigQuery View '{view_name}' 成功")
        except Exception as e:
            print(f"❌ 建立 BigQuery View '{view_name}' 失敗: {e}")
            raise

def create_table_from_view(view_name: str, table_name: str, dataset_id: str = DATASET_ID):
    """
    從 BigQuery View 建立實體 Table（類似 mysql.py 的 create_table_from_view）
    
    Args:
        view_name: 來源 View 的名稱
        table_name: 目標 Table 的名稱
        dataset_id: Dataset ID
    """
    client = get_bigquery_client()
    
    source_view_id = f"{PROJECT_ID}.{dataset_id}.{view_name}"
    dest_table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    try:
        # 完全取代：先刪除舊 Table，再建立新的
        print(f"🗑️  正在刪除舊的 BigQuery Table '{table_name}' (如果存在)...")
        try:
            client.delete_table(dest_table_id)
        except Exception:
            pass  # 表不存在時忽略錯誤
        
        print(f"📝 正在從 View '{view_name}' 建立新的 BigQuery Table '{table_name}'...")
        
        # 建立查詢工作來複製 view 資料到 table
        sql = f"SELECT * FROM `{source_view_id}`"
        
        job_config = bigquery.QueryJobConfig(
            destination=dest_table_id,
            write_disposition=WriteDisposition.WRITE_TRUNCATE
        )
        
        query_job = client.query(sql, job_config=job_config)
        query_job.result()  # 等待完成
        
        # 獲取記錄數量
        count_sql = f"SELECT COUNT(*) as count FROM `{dest_table_id}`"
        count_result = client.query(count_sql).result()
        count = list(count_result)[0].count
        
        print(f"✅ 成功建立 BigQuery Table '{table_name}'，共 {count} 筆記錄")
        
    except Exception as e:
        print(f"❌ 從 View '{view_name}' 建立 BigQuery Table '{table_name}' 失敗: {e}")
        raise

# ========== 具體業務邏輯函數 ==========

def create_course_sales_daily_view():
    """
    建立課程銷售日統計 View（對應 mysql.py 的同名函數）
    """
    view_sql = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_ID}.vw_course_sales_daily` AS
    SELECT
      t.course_id,
      DATE(t.captured_at) AS captured_date,
      t.price,
      t.sold_num,
      t.price * t.sold_num AS revenue
    FROM (
      SELECT
        s.*,
        ROW_NUMBER() OVER (
          PARTITION BY s.course_id, DATE(s.captured_at)
          ORDER BY s.sold_num DESC, s.captured_at DESC, s.id DESC
        ) AS rn
      FROM `{PROJECT_ID}.{DATASET_ID}.hahow_course_sales` s
      WHERE
        s.price < 999999
    ) AS t
    WHERE t.rn = 1
    """
    
    create_view("vw_course_sales_daily", view_sql)

# ========== 輔助函數 ==========

def execute_query(sql: str, dataset_id: str = DATASET_ID) -> List[Dict]:
    """
    執行 BigQuery SQL 查詢並返回結果
    
    Args:
        sql: SQL 查詢語句
        dataset_id: Dataset ID
    
    Returns:
        查詢結果的列表
    """
    client = get_bigquery_client()
    
    query_job = client.query(sql)
    results = query_job.result()
    
    # 直接轉換為字典列表
    return [dict(row) for row in results]

def get_table_info(table_name: str, dataset_id: str = DATASET_ID) -> Dict:
    """
    獲取 BigQuery 表的資訊
    
    Args:
        table_name: 表名
        dataset_id: Dataset ID
    
    Returns:
        表的資訊字典
    """
    client = get_bigquery_client()
    table_id = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    try:
        table = client.get_table(table_id)
        return {
            "table_id": table.table_id,
            "num_rows": table.num_rows,
            "num_bytes": table.num_bytes,
            "created": table.created,
            "modified": table.modified,
            "schema": [{"name": field.name, "type": field.field_type} for field in table.schema]
        }
    except Exception as e:
        print(f"❌ 獲取表 '{table_name}' 資訊失敗: {e}")
        raise

