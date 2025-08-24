
from data_ingestion.tasks_crawler_hahow_article import crawler_hahow_article
from data_ingestion.tasks_crawler_hahow_course import crawler_hahow_course

categories = ["programming", "marketing", "language"]

for category in categories:
    crawler_hahow_course.apply_async(
        kwargs={'category': category}, 
        queue='hahow_course'
    )
    print(f"send task to crawler_hahow_course: {category}")
    crawler_hahow_article.apply_async(
        kwargs={'category': category}, 
        queue='hahow_article'
    )
    print(f"send task to crawler_hahow_article: {category}")