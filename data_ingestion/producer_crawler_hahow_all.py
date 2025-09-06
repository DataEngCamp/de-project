from data_ingestion.tasks_crawler_hahow import crawler_hahow_article, crawler_hahow_course

categories = [
    "programming", "marketing", "language", "design", 
    "lifestyle", "music", "art", "photography", 'humanities',
    "finance-and-investment", "career-skills", "cooking",
]

for category in categories:
    crawler_hahow_course.delay(category=category)
    print(f"send task to crawler_hahow_course: {category}")
    crawler_hahow_article.delay(category=category)
    print(f"send task to crawler_hahow_article: {category}")