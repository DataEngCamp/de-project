"""
Sync MySQL to BigQuery Script
用於將 MySQL 資料同步到 BigQuery
"""
from data_ingestion.bigquery import (
    create_dataset_if_not_exists,
    create_table,
    upload_data_to_bigquery,
    PROJECT_ID,
    DATASET_ID
)

from data_ingestion.mysql import query_to_dataframe

from data_ingestion.bigquery import (
    hahow_course_bq_schema,
    hahow_course_sales_bq_schema,
    hahow_article_bq_schema
)


def sync_mysql_to_bigquery():
    """
    將 MySQL 資料同步到 BigQuery
    """
    # 確保 BigQuery Dataset 存在
    create_dataset_if_not_exists()
    
    # 同步的表配置
    tables_config = [
        {
            'mysql_table': 'hahow_course',
            'bq_table': 'hahow_course',
            'schema_func': hahow_course_bq_schema
        },
        {
            'mysql_table': 'hahow_course_sales',
            'bq_table': 'hahow_course_sales',
            'schema_func': hahow_course_sales_bq_schema
        },
        {
            'mysql_table': 'hahow_article',
            'bq_table': 'hahow_article',
            'schema_func': hahow_article_bq_schema
        }
    ]
    
    for config in tables_config:
        try:
            print(f"🔄 開始同步 {config['mysql_table']} 到 BigQuery...")
            
            # 建立 BigQuery 表（如果不存在）
            schema = config['schema_func']()
            create_table(table_name=config['bq_table'], schema=schema)
            
            # 從 MySQL 讀取資料
            sql = f"SELECT * FROM {config['mysql_table']}"
            df = query_to_dataframe(sql=sql)
            
            # 上傳到 BigQuery
            upload_data_to_bigquery(table_name=config['bq_table'], df=df, mode="replace")
            
            print(f"✅ {config['mysql_table']} 同步完成")
            
        except Exception as e:
            print(f"❌ {config['mysql_table']} 同步失敗: {e}")
            raise
    
    print("✅ 所有資料同步完成")


def main():
    """
    主函數，執行 MySQL 到 BigQuery 的同步操作
    """
    print("🚀 開始執行 MySQL 到 BigQuery 的同步...")
    sync_mysql_to_bigquery()
    print("🎉 MySQL 到 BigQuery 的同步完成！")


if __name__ == "__main__":
    main()
