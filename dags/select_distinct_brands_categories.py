from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging
import psycopg2
from psycopg2 import sql

# Настройка логирования
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'artzsh',
    'start_date': datetime(2024, 4, 27),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def get_vars(**kwargs):
    """
    Функция для получения переменных из Airflow Variables и отправки их в XCom.
    """
    ti = kwargs['ti']
    try:
        distinct_var = Variable.get("distinct_var")
        logger.info(f"Текущий ключ: distinct_var={distinct_var}")
    except KeyError:
        distinct_var = "1"
        Variable.set("distinct_var", distinct_var)
        logger.info(f"Переменная 'distinct_var' не найдена. Установлено значение по умолчанию: {distinct_var}")
    
    tables_json = Variable.get("tables_json", deserialize_json=True)
    ti.xcom_push(key='num', value=distinct_var)
    ti.xcom_push(key='tables_json', value=tables_json)
    logger.info(f"Получены переменные: distinct_var={distinct_var}, tables_json={tables_json}")

def create_unique_brands(**kwargs):
    """
    Функция для добавления уникальных брендов из текущей таблицы в целевую таблицу.
    """
    ti = kwargs['ti']
    
    # Извлечение данных из XCom
    tables_json = ti.xcom_pull(key='tables_json', task_ids='get_vars')
    key_str = ti.xcom_pull(key='num', task_ids='get_vars')
    
    logger.info(f"Обрабатываем ключ: {key_str}")
    dataset_info = tables_json.get(key_str)
    
    if not dataset_info:
        logger.info(f"Ключ '{key_str}' не найден. Завершение обработки брендов.")
        return "stop"
    
    source_brands_table = dataset_info.get('brands_table_name')
    main_schema = 'DDS'
    target_brands_table = 'unique_brands'  # Название целевой таблицы для брендов
    schema = 'DDS'  # Название схемы
    
    if not source_brands_table:
        raise ValueError(f"Не указано 'brands_table_name' для ключа '{key_str}'.")
    
    logger.info(f"Источник брендов: {schema}.{source_brands_table}")
    logger.info(f"Целевая таблица брендов: {schema}.{target_brands_table}")
    
    # Получение подключения к PostgreSQL
    conn_id = 'database_cloud'  # Замените, если используете другое Conn Id
    connection = BaseHook.get_connection(conn_id)
    
    postgres_conn_params = {
        'host': connection.host,
        'port': connection.port,
        'dbname': connection.schema,
        'user': connection.login,
        'password': connection.password
    }
    
    conn = None
    cursor = None
    
    try:
        # Установка соединения с PostgreSQL
        conn = psycopg2.connect(**postgres_conn_params)
        cursor = conn.cursor()
        logger.info("Соединение с PostgreSQL установлено.")
        
        # Создание целевой таблицы брендов, если она не существует
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {schema}.{target_table} (
                brand VARCHAR PRIMARY KEY
            );
        """).format(
            schema=sql.Identifier(schema),
            target_table=sql.Identifier(target_brands_table)
        )
        cursor.execute(create_table_query)
        conn.commit()
        logger.info(f"Целевая таблица '{schema}.{target_brands_table}' создана или уже существует.")
        
        # Подсчёт количества уникальных брендов в исходной таблице
        count_source_query = sql.SQL("""
            SELECT COUNT(DISTINCT brand) FROM {main_schema}.{source_table} WHERE brand IS NOT NULL;
        """).format(
            main_schema=sql.Identifier(main_schema),
            source_table=sql.Identifier(source_brands_table)
        )
        cursor.execute(count_source_query)
        source_count = cursor.fetchone()[0]
        logger.info(f"Количество уникальных брендов в источнике: {source_count}")
        
        # Подсчёт количества брендов в целевой таблице до вставки
        count_target_before_query = sql.SQL("""
            SELECT COUNT(*) FROM {schema}.{target_table};
        """).format(
            schema=sql.Identifier(schema),
            target_table=sql.Identifier(target_brands_table)
        )
        cursor.execute(count_target_before_query)
        target_count_before = cursor.fetchone()[0]
        logger.info(f"Количество брендов в целевой таблице до вставки: {target_count_before}")
        
        # Вставка уникальных брендов, которых еще нет в целевой таблице, с использованием ON CONFLICT DO NOTHING
        insert_query = sql.SQL("""
            INSERT INTO {schema}.{target_table} (brand)
            SELECT DISTINCT brand
            FROM {main_schema}.{source_table}
            WHERE brand IS NOT NULL
            AND brand NOT IN (SELECT brand FROM {schema}.{target_table});
        """).format(
            schema=sql.Identifier(schema),
            target_table=sql.Identifier(target_brands_table),
            main_schema=sql.Identifier(main_schema),
            source_table=sql.Identifier(source_brands_table)
        )
        cursor.execute(insert_query)
        inserted = cursor.rowcount if cursor.rowcount != -1 else 0  # rowcount может быть -1 для некоторых операций
        conn.commit()
        logger.info(f"Новые уникальные бренды добавлены в '{schema}.{target_brands_table}': {inserted} записей.")
        
        # Подсчёт количества брендов в целевой таблице после вставки
        count_target_after_query = sql.SQL("""
            SELECT COUNT(*) FROM {schema}.{target_table};
        """).format(
            schema=sql.Identifier(schema),
            target_table=sql.Identifier(target_brands_table)
        )
        cursor.execute(count_target_after_query)
        target_count_after = cursor.fetchone()[0]
        logger.info(f"Количество брендов в целевой таблице после вставки: {target_count_after}")
        
    except Exception as e:
        logger.error(f"Ошибка при добавлении брендов: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
            logger.info("Курсор закрыт.")
        if conn:
            conn.close()
            logger.info("Соединение с PostgreSQL закрыто.")

def create_unique_categories(**kwargs):
    """
    Функция для добавления уникальных категорий из текущей таблицы в целевую таблицу.
    """
    ti = kwargs['ti']
    
    # Извлечение данных из XCom
    tables_json = ti.xcom_pull(key='tables_json', task_ids='get_vars')
    key_str = ti.xcom_pull(key='num', task_ids='get_vars')
    
    logger.info(f"Обрабатываем ключ: {key_str}")
    dataset_info = tables_json.get(key_str)
    
    if not dataset_info:
        logger.info(f"Ключ '{key_str}' не найден. Завершение обработки категорий.")
        return "stop"
    
    source_categories_table = dataset_info.get('categories_table_name')
    main_schema = 'DDS'
    target_categories_table = 'unique_categories'  # Название целевой таблицы для категорий
    schema = 'DDS'  # Название схемы
    
    if not source_categories_table:
        raise ValueError(f"Не указано 'categories_table_name' для ключа '{key_str}'.")
    
    logger.info(f"Источник категорий: {schema}.{source_categories_table}")
    logger.info(f"Целевая таблица категорий: {schema}.{target_categories_table}")
    
    # Получение подключения к PostgreSQL
    conn_id = 'database_cloud'  # Замените, если используете другое Conn Id
    connection = BaseHook.get_connection(conn_id)
    
    postgres_conn_params = {
        'host': connection.host,
        'port': connection.port,
        'dbname': connection.schema,
        'user': connection.login,
        'password': connection.password
    }
    
    conn = None
    cursor = None
    
    try:
        # Установка соединения с PostgreSQL
        conn = psycopg2.connect(**postgres_conn_params)
        cursor = conn.cursor()
        logger.info("Соединение с PostgreSQL установлено.")
        
        # Создание целевой таблицы категорий, если она не существует
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {schema}.{target_table} (
                category_code VARCHAR PRIMARY KEY
            );
        """).format(
            schema=sql.Identifier(schema),
            target_table=sql.Identifier(target_categories_table)
        )
        cursor.execute(create_table_query)
        conn.commit()
        logger.info(f"Целевая таблица '{schema}.{target_categories_table}' создана или уже существует.")
        
        # Подсчёт количества уникальных категорий в исходной таблице
        count_source_query = sql.SQL("""
            SELECT COUNT(DISTINCT category_code) FROM {main_schema}.{source_table} WHERE category_code IS NOT NULL;
        """).format(
            main_schema=sql.Identifier(main_schema),
            source_table=sql.Identifier(source_categories_table)
        )
        cursor.execute(count_source_query)
        source_count = cursor.fetchone()[0]
        logger.info(f"Количество уникальных категорий в источнике: {source_count}")
        
        # Подсчёт количества категорий в целевой таблице до вставки
        count_target_before_query = sql.SQL("""
            SELECT COUNT(*) FROM {schema}.{target_table};
        """).format(
            schema=sql.Identifier(schema),
            target_table=sql.Identifier(target_categories_table)
        )
        cursor.execute(count_target_before_query)
        target_count_before = cursor.fetchone()[0]
        logger.info(f"Количество категорий в целевой таблице до вставки: {target_count_before}")
        
        # Вставка уникальных категорий, которых еще нет в целевой таблице, с использованием ON CONFLICT DO NOTHING
        insert_query = sql.SQL("""
            INSERT INTO {schema}.{target_table} (category_code)
            SELECT DISTINCT category_code
            FROM {main_schema}.{source_table}
            WHERE category_code IS NOT NULL
            AND category_code NOT IN (SELECT category_code FROM {schema}.{target_table});
        """).format(
            schema=sql.Identifier(schema),
            target_table=sql.Identifier(target_categories_table),
            main_schema=sql.Identifier(main_schema),
            source_table=sql.Identifier(source_categories_table)
        )
        cursor.execute(insert_query)
        inserted = cursor.rowcount if cursor.rowcount != -1 else 0  # rowcount может быть -1 для некоторых операций
        conn.commit()
        logger.info(f"Новые уникальные категории добавлены в '{schema}.{target_categories_table}': {inserted} записей.")
        
        # Подсчёт количества категорий в целевой таблице после вставки
        count_target_after_query = sql.SQL("""
            SELECT COUNT(*) FROM {schema}.{target_table};
        """).format(
            schema=sql.Identifier(schema),
            target_table=sql.Identifier(target_categories_table)
        )
        cursor.execute(count_target_after_query)
        target_count_after = cursor.fetchone()[0]
        logger.info(f"Количество категорий в целевой таблице после вставки: {target_count_after}")
        
    except Exception as e:
        logger.error(f"Ошибка при добавлении категорий: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cursor:
            cursor.close()
            logger.info("Курсор закрыт.")
        if conn:
            conn.close()
            logger.info("Соединение с PostgreSQL закрыто.")

def set_vars(**kwargs):
    """
    Функция для обновления индекса батча в Airflow Variables.
    """
    ti = kwargs['ti']
    current_num = ti.xcom_pull(key='num', task_ids='get_vars')
    tables_json = ti.xcom_pull(key='tables_json', task_ids='get_vars')
    if not tables_json:
        raise ValueError("Не удалось получить 'tables_json' из XCom.")
    
    if key_str := tables_json.get(current_num):
        # Есть следующий ключ, инкрементируем
        try:
            current_num_int = int(current_num)
            next_num = str(current_num_int + 1)
            Variable.set("distinct_var", next_num)
            logger.info(f"Обновлён distinct_var до {next_num}")
        except ValueError:
            logger.error(f"Текущее значение distinct_var '{current_num}' некорректно. Установка в '1'.")
            Variable.set("distinct_var", "1")
            logger.info("distinct_var установлена в '1'")
    else:
        # Нет следующего ключа, завершаем
        logger.info("Все ключи обработаны. Рекурсивный запуск прекращен.")

with DAG(
    'extract_unique_values',
    default_args=default_args,
    schedule_interval=None,  # Установите нужное расписание
    catchup=False,
) as dag:

    get_vars_task = PythonOperator(
        task_id='get_vars',
        python_callable=get_vars,
    )

    create_unique_brands_task = PythonOperator(
        task_id='create_unique_brands',
        python_callable=create_unique_brands,
        provide_context=True,
    )

    create_unique_categories_task = PythonOperator(
        task_id='create_unique_categories',
        python_callable=create_unique_categories,
        provide_context=True,
    )

    set_vars_task = PythonOperator(
        task_id='set_vars',
        python_callable=set_vars,
        provide_context=True,
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_extract_unique_values',
        trigger_dag_id='extract_unique_values',  # ID текущего DAG для рекурсивного запуска
        wait_for_completion=False,  # Установите True, если хотите ждать завершения DAG
        poke_interval=30,  # Интервал проверки статуса DAG (если wait_for_completion=True)
        conf={"message": "Запуск следующего шага обработки таблиц"},
    )

    # Установка зависимостей между задачами
    get_vars_task >> create_unique_brands_task >> create_unique_categories_task >> set_vars_task >> trigger_dag_task
