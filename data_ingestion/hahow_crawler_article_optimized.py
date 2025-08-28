import requests
import math
import time
import pandas as pd
from datetime import datetime, timezone

from data_ingestion.mysql import upload_data_to_mysql, upload_data_to_mysql_upsert, article_table
from data_ingestion.hahow_crawler_common import flatten_tag_names, to_datetime


def crawler_hahow_article(category: str):

    # 取得總頁數
    url = f'https://api.hahow.in/api/products/search?category=ARTICLE&groups={category}'
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    page_limit = response.json()['_metadata']['limit']
    total_count = response.json()['_metadata']['count']

    article_list = []
    # 爬取每一頁的資料
    for p in range(1, math.ceil(total_count / page_limit) + 1):
        url = f'https://api.hahow.in/api/products/search?category=ARTICLE&groups={category}&page={p}'
        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
        }
        response = requests.get(url, headers=headers)
        print(f"category: {category}, page:{p}, status_code:{response.status_code}")
        if response.status_code == 200:
            articles = response.json()['data']['articleData']['products']
            for article in articles:
                id = article.get('_id')
                type = article.get('type')
                title = article.get('title')
                group_title = article.get('group', {}).get('title')
                group_uniquename = article.get('group', {}).get('uniquename')
                subgroup_title = article.get('group', {}).get('subGroup', {}).get('title')
                subgroup_uniquename = article.get('group', {}).get('subGroup', {}).get('uniquename')
                link = 'https://hahow.in/contents/articles/' + str(article.get('_id'))
                tags = flatten_tag_names(article.get('productTags'))
                creator_name = article.get('creator', {}).get('name')
                view_count = article.get('viewCount')
                clap_total = article.get('clapTotal')
                preview_description = article.get('previewDescription')
                cover_image = article.get('coverImage', {}).get('url')
                created_at = to_datetime(article.get('createdAt'))
                updated_at = to_datetime(article.get('updatedAt'))
                publish_at = to_datetime(article.get('publishedAt'))

                article_dict = {
                    "id": id,
                    "category": category,
                    "type": type,
                    "title": title,
                    "group_title": group_title,
                    "group_uniquename": group_uniquename,
                    "subgroup_title": subgroup_title,
                    "subgroup_uniquename": subgroup_uniquename,
                    "link": link,
                    "tags": tags,
                    "creator_name": creator_name,
                    "view_count": view_count,
                    "clap_total": clap_total,
                    "preview_description": preview_description,
                    "cover_image": cover_image,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "publish_at": publish_at,
                }
                # print(article_dict)
                article_list.append(article_dict) # 每次新增單筆文章資料

        else:
            print(f"Fetch {url} failed.")
        
        time.sleep(0.1)

    df = pd.DataFrame(article_list)
    # df.to_csv(f"output/hahow_article_{category}.csv", index=False, encoding='utf-8-sig')
    # print(f"hahow_article_{category}.csv saved.")

    df['uploaded_at'] = datetime.now(timezone.utc)  # 新增 uploaded_at 欄位，設為現在時間
    upload_data_to_mysql(table_name="hahow_article", df=df)
    # data = df.to_dict(orient='records') # 將 DataFrame 轉換為字典列表
    # upload_data_to_mysql_upsert(table_obj=article_table, data=data)
    print(f"hahow_article_{category} has been uploaded to mysql.")


if __name__ == "__main__":
    crawler_hahow_article("programming")
