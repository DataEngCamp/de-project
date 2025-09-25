"""
Hahow Data Sync Script
用於在 BigQuery 中建立或更新檢視表和實體表
"""
from data_ingestion.bigquery import (
    create_view,
    create_table_from_view,
    PROJECT_ID,
    DATASET_ID
)


def create_course_sales_daily_view_and_table():
    """
    在 BigQuery 中建立課程銷售日統計 View 和 Table
    """
    view_sql = f"""
    SELECT
      t.course_id,
      DATE(t.captured_at) AS captured_date,
      t.price,
      t.sold_num,
      t.price * t.sold_num AS revenue
    FROM (
      SELECT
        s.*,
        ROW_NUMBER() OVER (
          PARTITION BY s.course_id, DATE(s.captured_at)
          ORDER BY s.sold_num DESC, s.captured_at DESC, s.id DESC
        ) AS rn
      FROM `{PROJECT_ID}.{DATASET_ID}.hahow_course_sales` s
      WHERE
        s.price < 999999
    ) AS t
    WHERE t.rn = 1
    """
    
    # 先建立 View
    create_view(view_name="vw_course_sales_daily", view_sql=view_sql)
    
    # 再從 View 建立實體 Table
    create_table_from_view(view_name="vw_course_sales_daily", table_name="hahow_course_sales_daily")
    
    print("✅ BigQuery 課程銷售日統計 View 和 Table 建立完成")


def create_advanced_analytics_view_and_table():
    """
    建立進階分析 View 和 Table - 課程趨勢分析
    """
    
    # 課程趨勢分析 View
    course_trend_sql = f"""
    SELECT
      course_id,
      captured_date,
      price,
      sold_num,
      revenue,
      LAG(sold_num) OVER (PARTITION BY course_id ORDER BY captured_date) as prev_sold_num,
      sold_num - LAG(sold_num) OVER (PARTITION BY course_id ORDER BY captured_date) as daily_sales,
      AVG(revenue) OVER (
        PARTITION BY course_id 
        ORDER BY captured_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
      ) as revenue_7day_avg
    FROM `{PROJECT_ID}.{DATASET_ID}.vw_course_sales_daily`
    """
    
    # 先建立 View
    create_view(view_name="vw_course_trend_analysis", view_sql=course_trend_sql)
    
    # 再從 View 建立實體 Table
    create_table_from_view(view_name="vw_course_trend_analysis", table_name="hahow_course_trend_analysis")
    
    print("✅ BigQuery 課程趨勢分析 View 和 Table 建立完成")


def create_daily_summary_view_and_table():
    """
    建立每日匯總 View 和 Table - 用於快速查詢和報表
    """
    # 從實體表建立每日匯總 View
    summary_sql = f"""
    SELECT
      captured_date,
      COUNT(DISTINCT course_id) as active_courses,
      SUM(revenue) as total_revenue,
      AVG(revenue) as avg_revenue_per_course,
      SUM(CASE WHEN daily_sales > 0 THEN daily_sales ELSE 0 END) as total_daily_sales,
      CURRENT_DATETIME() as created_at
    FROM `{PROJECT_ID}.{DATASET_ID}.hahow_course_trend_analysis`
    WHERE captured_date IS NOT NULL
    GROUP BY captured_date
    ORDER BY captured_date DESC
    """
    
    # 先建立 View
    create_view(view_name="vw_daily_summary", view_sql=summary_sql)
    
    # 再從 View 建立實體表
    create_table_from_view(view_name="vw_daily_summary", table_name="hahow_daily_summary")
    
    print("✅ BigQuery 每日匯總 View 和 Table 建立完成")


def main():
    """
    主函數，執行所有建立或更新操作
    """
    print("🚀 開始執行 Hahow Data Sync...")
    create_course_sales_daily_view_and_table()
    create_advanced_analytics_view_and_table()
    create_daily_summary_view_and_table()
    print("🎉 Hahow Data Sync 完成！")


if __name__ == "__main__":
    main()
