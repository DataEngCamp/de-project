import requests
import math
import time
import pandas as pd
from datetime import datetime, timezone

from data_ingestion.mysql import upload_data_to_mysql, upload_data_to_mysql_upsert, course_table
from data_ingestion.hahow_crawler_common import to_datetime


def crawler_hahow_course(category: str):

    # 取得總頁數
    url = f'https://api.hahow.in/api/products/search?category=COURSE&groups={category}'
    headers = {
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
    }
    response = requests.get(url, headers=headers)
    page_limit = response.json()['_metadata']['limit']
    total_count = response.json()['_metadata']['count']

    course_list = []
    sales_history_list = []
    current_time = datetime.now(timezone.utc)
    
    # 爬取每一頁的資料
    for p in range(1, math.ceil(total_count / page_limit) + 1):
        url = f'https://api.hahow.in/api/products/search?category=COURSE&groups={category}&page={p}'
        headers = {
            "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36"
        }
        response = requests.get(url, headers=headers)
        print(f"category: {category}, page:{p}, status_code:{response.status_code}")
        if response.status_code == 200:
            courses = response.json()['data']['courseData']['products']
            for course in courses:
                id = course.get('_id')
                uniquename = course.get('uniquename')
                title = course.get('title')
                status = course.get('status')
                link = 'https://hahow.in/courses/' + str(course.get('_id'))
                price = course.get('price')
                preordered_price = course.get('preOrderedPrice')
                average_rating = course.get('averageRating')
                num_rating = course.get('numRating')
                owner_name = course.get('owner', {}).get('name')
                sold_num = course.get('numSoldTickets')
                bookmark_count = course.get('bookmarkCount')
                meta_description = course.get('metaDescription')
                cover_image = course.get('coverImage', {}).get('url')
                incubate_time = to_datetime(course.get('incubateTime'))
                publish_time = to_datetime(course.get('publishTime'))
                video_length = course.get('totalVideoLengthInSeconds')

                course_dict = {
                    "id": id,
                    "category": category,
                    "uniquename": uniquename,
                    "title": title,
                    "status": status,
                    "link": link,
                    "price": price,
                    "preordered_price": preordered_price,
                    "average_rating": average_rating,
                    "num_rating": num_rating,
                    "owner_name": owner_name,
                    "sold_num": sold_num,
                    "bookmark_count": bookmark_count,
                    "meta_description": meta_description,
                    "cover_image": cover_image,
                    "incubate_time": incubate_time,
                    "publish_time": publish_time,
                    "video_length": video_length,
                }
                # print(course_dict)
                course_list.append(course_dict) # 每次新增單筆課程資料


                # 銷售歷史資料（用於插入歷史記錄）
                course_sales_dict = {
                    "course_id": id,
                    "price": price,
                    "sold_num": sold_num,
                    "captured_at": current_time,
                }
                sales_history_list.append(course_sales_dict) # 每次新增單筆銷售歷史資料

        else:
            print(f"Fetch {url} failed.")

        time.sleep(0.1)


    now_time = datetime.now(timezone.utc)

    df = pd.DataFrame(course_list)
    df['uploaded_at'] = now_time  # 新增 uploaded_at 欄位，設為現在時間
    # 不能使用 replace 模式上傳，不同類別的課程資料會被覆蓋
    # upload_data_to_mysql(table_name="hahow_course", df=df, mode="replace")
    upload_data_to_mysql_upsert(table_obj=course_table, data=df)
    print(f"hahow_course_{category} has been uploaded to mysql.")

    df_sales = pd.DataFrame(sales_history_list)
    df_sales['uploaded_at'] = now_time  # 新增 uploaded_at 欄位，設為現在時間
    upload_data_to_mysql(table_name="hahow_course_sales", df=df_sales, mode="append")
    print(f"hahow_course_sales_{category} uploaded to mysql.")


if __name__ == "__main__":
    crawler_hahow_course("programming")
