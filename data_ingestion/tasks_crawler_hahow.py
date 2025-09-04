"""
Hahow 爬蟲整合任務
整合 course 和 article 爬蟲功能，註冊為 Celery 任務
"""
from data_ingestion.worker import app
from data_ingestion.hahow_crawler_course import crawler_hahow_course as _crawler_hahow_course
from data_ingestion.hahow_crawler_article import crawler_hahow_article as _crawler_hahow_article


@app.task()
def crawler_hahow_course(category: str, **kwargs):
    """
    Celery 任務版本的 Hahow 課程爬蟲
    
    Args:
        category (str): 課程分類 (如: programming, marketing, language)
        **kwargs: 其他參數
    """
    print(f"Starting Hahow course crawler for category: {category}")
    _crawler_hahow_course(category)
    print(f"Completed Hahow course crawler for category: {category}")


@app.task()
def crawler_hahow_article(category: str, **kwargs):
    """
    Celery 任務版本的 Hahow 文章爬蟲
    
    Args:
        category (str): 文章分類 (如: programming, marketing, language)
        **kwargs: 其他參數
    """
    print(f"Starting Hahow article crawler for category: {category}")
    _crawler_hahow_article(category)
    print(f"Completed Hahow article crawler for category: {category}")
