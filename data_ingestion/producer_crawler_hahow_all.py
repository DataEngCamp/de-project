from data_ingestion.tasks_crawler_hahow_article import crawler_hahow_article
from data_ingestion.tasks_crawler_hahow_course import crawler_hahow_course

categories = ["programming", "marketing", "language"]

for category in categories:
    crawler_hahow_course.delay(category=category)
    print(f"send task to crawler_hahow_course: {category}")
    crawler_hahow_article.delay(category=category)
    print(f"send task to crawler_hahow_article: {category}")