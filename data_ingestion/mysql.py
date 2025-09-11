import pandas as pd
from sqlalchemy import create_engine, text  # 建立資料庫連線的工具（SQLAlchemy）
from sqlalchemy import Column, Float, MetaData, String, Table, Integer, Text, DECIMAL, DATETIME
from sqlalchemy.dialects.mysql import insert

from data_ingestion.config import MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT

MYSQL_DATABASE = "hahow"

# 創建元資料
metadata = MetaData()

# 課程表結構
course_table = Table(
    "hahow_course",  # 資料表名稱
    metadata,
    Column("id", String(50), primary_key=True, comment="課程ID"),
    Column("category", String(100), nullable=True, comment="課程分類"),
    Column("uniquename", String(255), nullable=True, comment="課程唯一名稱"),
    Column("title", String(500), nullable=True, comment="課程標題"),
    Column("status", String(50), nullable=True, comment="課程狀態"),
    Column("link", String(500), nullable=True, comment="課程連結"),
    Column("price", DECIMAL(10, 2), nullable=True, comment="課程價格"),
    Column("preordered_price", DECIMAL(10, 2), nullable=True, comment="預購價格"),
    Column("average_rating", Float, nullable=True, comment="平均評分"),
    Column("num_rating", Integer, nullable=True, default=0, comment="評分數量"),
    Column("owner_name", String(255), nullable=True, comment="講師姓名"),
    Column("sold_num", Integer, nullable=True, default=0, comment="銷售數量"),
    Column("bookmark_count", Integer, nullable=True, default=0, comment="收藏數量"),
    Column("meta_description", Text, nullable=True, comment="課程描述"),
    Column("cover_image", String(1000), nullable=True, comment="封面圖片URL"),
    Column("incubate_time", DATETIME, nullable=True, comment="課程孵化時間"),
    Column("publish_time", DATETIME, nullable=True, comment="課程發布時間"),
    Column("video_length", Integer, nullable=True, comment="影片長度(秒)"),
    Column("uploaded_at", DATETIME, nullable=False, comment="資料上傳時間"),
)

# 課程銷售歷史記錄表 - 儲存每次抓取時的價格和購買人數
course_sales_table = Table(
    "hahow_course_sales",  # 資料表名稱
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True, comment="自動遞增主鍵"),
    Column("course_id", String(50), nullable=False, comment="課程ID，關聯到 hahow_course.id"),
    Column("price", DECIMAL(10, 2), nullable=True, comment="抓取時的課程價格"),
    Column("sold_num", Integer, nullable=True, default=0, comment="抓取時的銷售數量"),
    Column("captured_at", DATETIME, nullable=False, comment="資料抓取時間"),
    Column("uploaded_at", DATETIME, nullable=False, comment="資料上傳時間"),
)

# 文章表結構
article_table = Table(
    "hahow_article",  # 資料表名稱
    metadata,
    Column("id", String(50), primary_key=True, comment="文章ID"),
    Column("category", String(100), nullable=True, comment="文章分類"),
    Column("type", String(50), nullable=True, comment="文章類型"),
    Column("title", String(500), nullable=True, comment="文章標題"),
    Column("group_title", String(255), nullable=True, comment="類別標題"),
    Column("group_uniquename", String(255), nullable=True, comment="類別唯一名稱"),
    Column("subgroup_title", String(255), nullable=True, comment="子類別標題"),
    Column("subgroup_uniquename", String(255), nullable=True, comment="子類別唯一名稱"),
    Column("link", String(500), nullable=True, comment="文章連結"),
    Column("tags", Text, nullable=True, comment="文章標籤"),
    Column("creator_name", String(255), nullable=True, comment="創作者姓名"),
    Column("view_count", Integer, nullable=True, default=0, comment="觀看次數"),
    Column("clap_total", Integer, nullable=True, default=0, comment="拍手總數"),
    Column("preview_description", Text, nullable=True, comment="預覽描述"),
    Column("cover_image", String(1000), nullable=True, comment="封面圖片URL"),
    Column("created_at", DATETIME, nullable=True, comment="文章建立時間"),
    Column("updated_at", DATETIME, nullable=True, comment="文章更新時間"),
    Column("publish_at", DATETIME, nullable=True, comment="文章發布時間"),
    Column("uploaded_at", DATETIME, nullable=False, comment="資料上傳時間"),
)


def upload_data_to_mysql(table_name: str, df: pd.DataFrame, mode: str = "replace"):
    """
    上傳 DataFrame 到 MySQL（使用全域引擎和適當的連接管理）
    """
    mysql_address = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    engine = create_engine(mysql_address)
    
    # ✅ 使用 context manager 確保連接會被正確關閉
    with engine.connect() as connection:
        df.to_sql(
            table_name,
            con=connection,
            if_exists=mode,
            index=False,
        )
    print(f"✅ 資料已上傳到表 '{table_name}'，共 {len(df)} 筆記錄")


def upload_data_to_mysql_upsert(table_obj: Table, data: list[dict]):
    mysql_address = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    engine = create_engine(mysql_address)
    
    # ✅ 自動建立資料表（如果不存在才建立）
    metadata.create_all(engine, tables=[table_obj])

    # upsert
    with engine.begin() as connection:
        for row in data:
            insert_stmt = insert(table_obj).values(**row)
            update_dict = {
                col.name: insert_stmt.inserted[col.name]
                for col in table_obj.columns
            }
            upsert_stmt = insert_stmt.on_duplicate_key_update(**update_dict)
            connection.execute(upsert_stmt)
    print(f"✅ UPSERT 完成，處理 {len(data)} 筆記錄到表 '{table_obj.name}'")


def upload_data_to_mysql_insert(table_obj: Table, data: list[dict]):
    """使用 SQLAlchemy INSERT 上傳資料到 MySQL"""
    mysql_address = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    engine = create_engine(mysql_address)
    
    # 自動建立資料表（如果不存在才建立）
    metadata.create_all(engine, tables=[table_obj])
    
    with engine.begin() as connection:
        for row in data:
            insert_stmt = insert(table_obj).values(**row)
            connection.execute(insert_stmt)
    
    print(f"✅ INSERT 完成，處理 {len(data)} 筆記錄到表 '{table_obj.name}'")


def create_view(view_name: str, view_sql: str):
    """
    在 MySQL 中建立或替換 View
    
    Args:
        view_name: View 的名稱
        view_sql: 建立 View 的 SQL 語句
    """
    mysql_address = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    engine = create_engine(mysql_address)
    
    with engine.begin() as connection:
        try:
            # 執行建立 View 的 SQL
            connection.execute(text(view_sql))
            print(f"✅ View '{view_name}' 建立成功")
        except Exception as e:
            print(f"❌ 建立 View '{view_name}' 失敗: {e}")
            raise


def create_course_sales_daily_view():
    """
    建立課程銷售日統計 View
    """
    view_sql = """
    CREATE OR REPLACE VIEW vw_course_sales_daily AS
    SELECT
      t.course_id,
      DATE(t.captured_at) AS captured_date,
      t.price,
      t.sold_num
    FROM (
      SELECT
        s.*,
        ROW_NUMBER() OVER (
          PARTITION BY s.course_id, DATE(s.captured_at)
          ORDER BY s.sold_num DESC, s.captured_at DESC, s.id DESC
        ) AS rn
      FROM hahow_course_sales s
    ) AS t
    WHERE t.rn = 1;
    """
    
    create_view("vw_course_sales_daily", view_sql)


def create_table_from_view(view_name: str, table_name: str):
    """
    使用純 SQL 從 View 中撈取資料並建立/取代實體 Table
    
    Args:
        view_name: 來源 View 的名稱
        table_name: 目標 Table 的名稱
    """
    mysql_address = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    engine = create_engine(mysql_address)
    
    with engine.begin() as connection:
        try:
            # 完全取代：先刪除舊 Table，再建立新的
            print(f"🗑️  正在刪除舊的 Table '{table_name}' (如果存在)...")
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            
            print(f"📝 正在從 View '{view_name}' 建立新的 Table '{table_name}'...")
            create_table_sql = f"CREATE TABLE {table_name} AS SELECT * FROM {view_name}"
            connection.execute(text(create_table_sql))
            
            # 獲取記錄數量
            result = connection.execute(text(f"SELECT COUNT(*) as count FROM {table_name}"))
            count = result.fetchone()[0]
            
            print(f"✅ 成功建立 Table '{table_name}'，共 {count} 筆記錄")
            
        except Exception as e:
            print(f"❌ 從 View '{view_name}' 建立 Table '{table_name}' 失敗: {e}")
            raise
