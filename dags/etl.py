from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import sql
from datetime import datetime
import gzip
import requests
from io import StringIO, BytesIO, TextIOWrapper
import time
import logging

default_args = {
    'owner': 'artzsh',
    'start_date':datetime(2018, 11, 1),
    'provide_context':True
}

def get_vars(**kwargs):
    ti = kwargs['ti']
    test_var = Variable.get("test_var")
    tables_json = Variable.get("tables_json", deserialize_json = True)
    ti.xcom_push(key='num', value=test_var)
    ti.xcom_push(key='tables_json', value=tables_json)
    
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

# Настройка логирования
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

def load_csv_to_postgres(**kwargs):
    """
    Функция для загрузки CSV или CSV.GZ файла из интернета в PostgreSQL с использованием COPY.
    Вставляет данные в фиксированную схему PostgreSQL.
    
    Параметры:
    - **kwargs: Передаваемые аргументы Airflow (используются для доступа к XCom).
    """
    ti = kwargs['ti']
    
    # Получение переменных из XCom
    tables_json = ti.xcom_pull(key='tables_json', task_ids=['get_vars'])[0]
    num = ti.xcom_pull(key='num', task_ids=['get_vars'])
    key_str = num[0]
    
    logger.info(f"Обрабатываем ключ: {key_str}")
    logger.info(f"Информация о таблице: {tables_json}")
    
    # Извлечение информации о датасете
    dataset_info = tables_json.get(key_str)
    if not dataset_info:
        raise ValueError(f"Ключ '{key_str}' не найден в tables_json.")
    
    dataset_url = dataset_info.get('dataset_url')
    table_name = dataset_info.get('table_name')
    schema_name = 'DDS-STG'  # Замените на название вашей схемы
    
    if not dataset_url or not table_name:
        raise ValueError(f"Неверные параметры для ключа '{key_str}'. Необходимо указать 'dataset_url' и 'table_name'.")
    
    logger.info(f"URL датасета: {dataset_url}")
    logger.info(f"Название таблицы: {schema_name}.{table_name}")
    
    # Получение подключения к PostgreSQL из Airflow Connections
    conn_id = 'database_cloud'  # Замените, если используете другое Conn Id
    connection = BaseHook.get_connection(conn_id)
    
    # Формирование параметров подключения
    postgres_conn_params = {
        'host': connection.host,
        'port': connection.port,
        'dbname': connection.schema,
        'user': connection.login,
        'password': connection.password
    }
    
    # Определение, является ли файл сжатым
    is_gzipped = dataset_url.endswith('.gz')
    logger.info(f"Файл сжат: {is_gzipped}")
    
    start_time = time.time()
    total_rows = 0
    chunk_number = 0
    conn = None
    cursor = None
    
    try:
        # Шаг 1: Скачивание CSV-файла
        logger.info(f"Скачивание CSV-файла с URL: {dataset_url}")
        response = requests.get(dataset_url, stream=True)
        response.raise_for_status()
        logger.info("Файл успешно скачан.")
        
        # Шаг 2: Декомпрессия (если необходимо) и чтение CSV-файла с использованием pandas в чанках
        logger.info("Начало чтения CSV-файла в чанках.")
        if is_gzipped:
            # Оборачиваем поток в GzipFile для декомпрессии
            decompressed_stream = gzip.GzipFile(fileobj=response.raw)
            csv_reader = pd.read_csv(decompressed_stream, chunksize=100000)
        else:
            # Если файл не сжат, читаем напрямую
            csv_reader = pd.read_csv(response.raw, chunksize=100000)
        
        # Шаг 3: Установка соединения с PostgreSQL (только для первого чанка)
        logger.info("Установка соединения с PostgreSQL.")
        conn = psycopg2.connect(**postgres_conn_params)
        cursor = conn.cursor()
        logger.info("Соединение установлено.")
        
        # Шаг 4: Создание таблицы (если не существует) на основе первого чанка
        try:
            first_chunk = next(csv_reader)
        except StopIteration:
            logger.info("CSV-файл пуст или не содержит данных.")
            return
        
        chunk_number += 1
        rows_in_chunk = len(first_chunk)
        logger.info(f"Обработка {rows_in_chunk} строк (первый чанк).")
        
        # Определение типов данных
        columns = first_chunk.columns
        column_types = []
        for dtype in first_chunk.dtypes:
            if pd.api.types.is_integer_dtype(dtype):
                column_types.append("BIGINT")  # Используем BIGINT для больших чисел
            elif pd.api.types.is_float_dtype(dtype):
                column_types.append("FLOAT")
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                column_types.append("TIMESTAMP")
            else:
                column_types.append("TEXT")
        
        # Создание таблицы
        logger.info(f"Создание таблицы '{schema_name}.{table_name}' (если не существует).")
        columns_with_types = ", ".join([f'"{col}" {ctype}' for col, ctype in zip(columns, column_types)])
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                {fields}
            );
        """).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name),
            fields=sql.SQL(columns_with_types)
        )
        cursor.execute(create_table_query)
        conn.commit()
        logger.info(f"Таблица '{schema_name}.{table_name}' готова для загрузки данных.")
        
        # Шаг 5: Загрузка первого чанка с использованием COPY
        logger.info(f"Загрузка данных в таблицу '{schema_name}.{table_name}' с использованием COPY.")
        bytes_io = BytesIO()
        text_wrapper = TextIOWrapper(bytes_io, encoding='utf-8')
        first_chunk.to_csv(text_wrapper, index=False, header=False)
        text_wrapper.flush()
        bytes_io.seek(0)
        
        copy_sql = sql.SQL("""
            COPY {schema}.{table} FROM STDIN WITH CSV
        """).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name)
        )
        
        start_chunk_time = time.time()
        cursor.copy_expert(sql=copy_sql, file=bytes_io)
        conn.commit()
        end_chunk_time = time.time()
        chunk_elapsed = end_chunk_time - start_chunk_time
        total_rows += rows_in_chunk
        logger.info(f"Чанк {chunk_number} загружен: {rows_in_chunk} строк за {chunk_elapsed:.2f} секунд. Всего загружено: {total_rows} строк.")
        bytes_io.close()
        
        # Шаг 6: Обработка остальных чанков
        for chunk in csv_reader:
            chunk_number += 1
            rows_in_chunk = len(chunk)
            logger.info(f"Обработка {rows_in_chunk} строк.")
            
            logger.info(f"Загрузка данных в таблицу '{schema_name}.{table_name}' с использованием COPY.")
            bytes_io = BytesIO()
            text_wrapper = TextIOWrapper(bytes_io, encoding='utf-8')
            chunk.to_csv(text_wrapper, index=False, header=False)
            text_wrapper.flush()
            bytes_io.seek(0)
            
            start_chunk_time = time.time()
            cursor.copy_expert(sql=copy_sql, file=bytes_io)
            conn.commit()
            end_chunk_time = time.time()
            chunk_elapsed = end_chunk_time - start_chunk_time
            total_rows += rows_in_chunk
            logger.info(f"Чанк {chunk_number} загружен: {rows_in_chunk} строк за {chunk_elapsed:.2f} секунд. Всего загружено: {total_rows} строк.")
            bytes_io.close()
        
        end_time = time.time()
        total_elapsed = end_time - start_time
        logger.info(f"Все данные успешно загружены. Всего вставлено строк: {total_rows}. Общее время выполнения: {total_elapsed:.2f} секунд.")
    
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP ошибка при скачивании файла: {http_err}")
        raise
    except StopIteration:
        logger.info("CSV-файл пуст или не содержит данных.")
    except Exception as err:
        logger.error(f"Произошла ошибка: {err}")
        if conn:
            conn.rollback()
        raise
    finally:
        # Закрытие соединения
        if 'cursor' in locals() and cursor:
            cursor.close()
            logger.info("Курсор закрыт.")
        if 'conn' in locals() and conn:
            conn.close()
            logger.info("Соединение с PostgreSQL закрыто.")

def clean_table(**kwargs):
    """
    Функция для удаления строк из основной таблицы, где brand или category_code равны NULL.
    
    Параметры:
    - **kwargs: Передаваемые аргументы Airflow (используются для доступа к XCom).
    """
    ti = kwargs['ti']
    
    # Получение переменных из XCom
    tables_json = ti.xcom_pull(key='tables_json', task_ids=['get_vars'])[0]
    num = ti.xcom_pull(key='num', task_ids=['get_vars'])
    key_str = num[0]
    
    print(f"[INFO] Обрабатываем ключ: {key_str}")
    print(f"[INFO] Информация о таблице: {tables_json}")
    
    # Извлечение информации о датасете
    dataset_info = tables_json.get(key_str)
    if not dataset_info:
        raise ValueError(f"Ключ '{key_str}' не найден в tables_json.")
    
    table_name = dataset_info.get('table_name')
    if not table_name:
        raise ValueError(f"Неверные параметры для ключа '{key_str}'. Необходимо указать 'table_name'.")
    
    # Фиксированная схема
    schema_name = 'DDS-STG'  # Замените на название вашей схемы
    
    print(f"[INFO] Основная таблица: {schema_name}.{table_name}")
    
    # Получение подключения к PostgreSQL из Airflow Connections
    conn_id = 'database_cloud'  # Замените, если используете другое Conn Id
    connection = BaseHook.get_connection(conn_id)
    
    # Формирование параметров подключения
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
        print("[INFO] Установка соединения с PostgreSQL.")
        conn = psycopg2.connect(**postgres_conn_params)
        cursor = conn.cursor()
        print("[INFO] Соединение установлено.")
        
        # Шаг 1: Удаление строк, где brand IS NULL
        delete_brand_null_query = sql.SQL("""
            DELETE FROM {schema}.{table}
            WHERE brand IS NULL;
        """).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name)
        )
        print("[INFO] Удаление строк, где brand IS NULL.")
        cursor.execute(delete_brand_null_query)
        deleted_brand_null = cursor.rowcount
        conn.commit()
        print(f"[SUCCESS] Удалены {deleted_brand_null} строк с brand IS NULL.")
        
        # Шаг 2: Удаление строк, где category_code IS NULL
        delete_category_null_query = sql.SQL("""
            DELETE FROM {schema}.{table}
            WHERE category_code IS NULL;
        """).format(
            schema=sql.Identifier(schema_name),
            table=sql.Identifier(table_name)
        )
        print("[INFO] Удаление строк, где category_code IS NULL.")
        cursor.execute(delete_category_null_query)
        deleted_category_null = cursor.rowcount
        conn.commit()
        print(f"[SUCCESS] Удалены {deleted_category_null} строк с category_code IS NULL.")
    
    except Exception as e:
        print(f"[ERROR] Произошла ошибка при удалении строк: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        # Закрытие соединения
        if cursor:
            cursor.close()
            print("[INFO] Курсор закрыт.")
        if conn:
            conn.close()
            print("[INFO] Соединение с PostgreSQL закрыто.")
    
    print(f"[COMPLETE] Удаление строк с NULL завершено для таблицы '{schema_name}.{table_name}'.")

with DAG(
    'ETL',
    default_args=default_args,
    schedule_interval=None,  # Установите нужное расписание
    catchup=False,
) as dag:

    get_vars = PythonOperator(
        task_id='get_vars',
        python_callable=get_vars,
    )

    load_csv_to_postgres = PythonOperator(
        task_id = 'load_csv_to_postgres',
        python_callable=load_csv_to_postgres
    )

    clean_table = PythonOperator(
        task_id = 'clean_table',
        python_callable=clean_table
    )

    trigger_dag_extract_tables = TriggerDagRunOperator(
        task_id='trigger_dag_extract_tables',
        trigger_dag_id='extract_tables',  # ID DAG'а, который нужно запустить
        wait_for_completion=False,  # Установите True, если хотите ждать завершения DAG B
        poke_interval=30,  # Интервал проверки статуса DAG B (если wait_for_completion=True)
        conf={"message": "Запуск DAG extract_tables после успешного завершения DAG ETL"},
    )
    


    get_vars >> load_csv_to_postgres >> clean_table >> trigger_dag_extract_tables
