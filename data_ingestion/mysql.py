import pandas as pd
from sqlalchemy import create_engine  # 建立資料庫連線的工具（SQLAlchemy）
from sqlalchemy import func

from data_ingestion.config import MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_PORT

MYSQL_DATABASE = "hahow"


def upload_data_to_mysql(table_name: str, df: pd.DataFrame):
    """
    上傳 DataFrame 到 MySQL
    """
    mysql_address = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"
    engine = create_engine(mysql_address)   
    
    # ✅ 使用 context manager 確保連接會被正確關閉
    with engine.connect() as connection:
        df.to_sql(
            table_name,
            con=connection,
            if_exists="append",
            index=False,
        )
        print(f"✅ 資料已上傳到表 '{table_name}'，共 {len(df)} 筆記錄")
