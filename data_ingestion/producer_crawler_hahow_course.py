from data_ingestion.tasks_crawler_hahow_course import crawler_hahow_course

categories = ["programming", "marketing", "language"]

for category in categories:
    crawler_hahow_course.delay(category=category)