from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'artzsh',
    'start_date': datetime(2024, 4, 27),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'create_dm_tables',
    default_args=default_args,
    schedule_interval=None,  # Установите нужное расписание
    catchup=False,
) as dag:

    create_conversion_rates_holidays = PostgresOperator(
        task_id='create_conversion_rates_holidays',
        postgres_conn_id='database_cloud',
        sql="""
            CREATE TABLE "DM".conversion_rates_holidays AS
            WITH RankedEvents AS ( 
                SELECT 
                    ft.user_id, 
                    ft.user_session, 
                    ft.event_time, 
                    ft.event_type, 
                    ft.product_id 
                FROM "DDS".final_table ft 
                JOIN "DDS".holidays h ON ft.event_time BETWEEN h.start_date AND h.end_date 
                WHERE h.holiday IN ('New Year', 'Black Friday', 'Valentines Day', 'International Womens Day', 'Defender of the Fatherland Day') 
                    AND ft.event_type IN ('cart', 'purchase') 
            ), 
            FilteredEvents AS ( 
                SELECT 
                    user_id, 
                    user_session, 
                    product_id, 
                    MIN(CASE WHEN event_type = 'cart' THEN event_time END) as added_to_cart_time, 
                    MAX(CASE WHEN event_type = 'purchase' THEN event_time END) as purchased_time 
                FROM RankedEvents 
                GROUP BY user_id, user_session, product_id 
                HAVING COUNT(*) = 2 
            ), 
            Purchase_Hol AS ( 
                SELECT count(*) as holpurchase_count 
                FROM FilteredEvents 
                WHERE added_to_cart_time < purchased_time 
            ), 
            Cart_Hol AS ( 
                SELECT count(*) as holcart_count 
                FROM "DDS".final_table ft  
                JOIN "DDS".holidays h ON ft.event_time BETWEEN h.start_date AND h.end_date  
                WHERE h.holiday IN ('Black Friday', 'New Year', 'Valentines Day', 'International Womens Day', 'Defender of the Fatherland Day') 
                    AND ft.event_type = 'cart' 
            ), 
            Purchase_all AS ( 
                SELECT count(*) as overallpurchase 
                FROM "DDS".final_table ft  
                WHERE ft.event_type = 'purchase' 
            ), 
            Cart_all AS ( 
                SELECT count(*) as overallcart 
                FROM "DDS".final_table ft  
                WHERE ft.event_type = 'cart' 
            ) 
            SELECT 
                (holpurchase_count::numeric / holcart_count) as holiday_conversion,  
                ((overallpurchase - holpurchase_count)::numeric / (overallcart - holcart_count)) as overall_conversion 
            FROM Purchase_Hol, Cart_Hol, Purchase_all, Cart_all;
        """
    )

    create_categories_conversion_rates = PostgresOperator(
        task_id='create_categories_conversion_rates',
        postgres_conn_id='database_cloud',
        sql="""
            CREATE TABLE "DM".categories_conversion_rates AS
            SELECT 
                split_part(category_code, '.', 1) AS top_category, 
                COUNT(*) FILTER (WHERE event_type = 'cart') AS add_to_cart_count, 
                COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchase_count, 
                ROUND( 
                    COUNT(*) FILTER (WHERE event_type = 'purchase')::decimal / NULLIF(COUNT(*) FILTER (WHERE event_type = 'cart'), 0), 
                    4 
                ) AS conversion_rate,
                CASE 
                    WHEN split_part(category_code, '.', 1) IN ('auto', 'computers', 'medicine', 'construction') THEN 'Узконаправленные товары'
                    ELSE 'Широконаправленные'
                END AS category
            FROM 
                "DDS".final_table 
            WHERE 
                event_type IN ('cart', 'purchase') AND split_part(category_code, '.', 1) != 'electronics' 
            GROUP BY 
                top_category 
            HAVING 
                COUNT(*) FILTER (WHERE event_type = 'cart') > 0 
            ORDER BY 
                conversion_rate DESC;
        """
    )

    create_black_friday_sales = PostgresOperator(
        task_id='create_black_friday_sales',
        postgres_conn_id='database_cloud',
        sql="""
            CREATE TABLE "DM".black_friday_sales AS
            WITH novembertable AS (
                SELECT 
                    count(*) AS purchases, 
                    DATE(ft.event_time) AS day
                FROM "DDS".final_table ft 
                WHERE ft.event_type = 'purchase' 
                  AND DATE(ft.event_time) BETWEEN '2019-11-10' AND '2019-12-03'
                GROUP BY DATE(ft.event_time)
            )
            SELECT 
                purchases, 
                day,
                CASE 
                    WHEN day BETWEEN '2019-11-22' AND '2019-12-02' THEN 'Black Friday'
                    WHEN day = '2019-12-03' THEN 'Last sale day'
                    ELSE 'no sale'
                END AS saleday
            FROM novembertable
            LEFT JOIN "DDS".holidays h 
                ON h."date" = novembertable.day
            ORDER BY day;
        """
    )

    create_conversion_rates_holidays >> create_categories_conversion_rates >> create_black_friday_sales
