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


def create_brands_table(**kwargs):
    """
    Функция для создания таблицы с уникальными брендами в PostgreSQL.
    
    Параметры:
    - schema_name: str, название схемы в PostgreSQL.
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
    brands_table_name = dataset_info.get('brands_table_name')
    
    if not table_name or not brands_table_name:
        raise ValueError(f"Неверные параметры для ключа '{key_str}'. Необходимо указать 'table_name' и 'brands_table_name'.")
    
    # Получение названия схемы из op_kwargs
    schema_name = 'DDS'
    main_schema_name = 'DDS-STG'
    if not schema_name:
        raise ValueError("Не указано название схемы (schema_name).")
    
    print(f"[INFO] Основная таблица: {schema_name}.{table_name}")
    print(f"[INFO] Таблица брендов: {schema_name}.{brands_table_name}")
    
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
        
        # Создание таблицы брендов с уникальными значениями
        print(f"[INFO] Создание таблицы брендов '{schema_name}.{brands_table_name}' с уникальными брендами.")
        
        # Формирование SQL-запроса
        create_brands_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {schema}.{brands_table} AS
            SELECT DISTINCT brand
            FROM {main_schema}.{main_table}
            WHERE brand IS NOT NULL;
        """).format(
            schema=sql.Identifier(schema_name),
            brands_table=sql.Identifier(brands_table_name),
            main_table=sql.Identifier(table_name),
            main_schema=sql.Identifier(main_schema_name)
        )
        
        # Выполнение запроса
        cursor.execute(create_brands_table_query)
        conn.commit()
        print(f"[INFO] Таблица '{schema_name}.{brands_table_name}' успешно создана или обновлена.")
    
    except Exception as e:
        print(f"[ERROR] Произошла ошибка при создании таблицы брендов: {e}")
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
    
    print(f"[COMPLETE] Таблица '{schema_name}.{brands_table_name}' с уникальными брендами создана.")

def create_categories_table(**kwargs):
    """
    Функция для создания таблицы с уникальными брендами в PostgreSQL.
    
    Параметры:
    - schema_name: str, название схемы в PostgreSQL.
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
    categories_table_name = dataset_info.get('categories_table_name')
    
    if not table_name or not categories_table_name:
        raise ValueError(f"Неверные параметры для ключа '{key_str}'. Необходимо указать 'table_name' и 'categories_table_name'.")
    
    # Получение названия схемы из op_kwargs
    schema_name = 'DDS'
    main_schema_name = 'DDS-STG'
    if not schema_name:
        raise ValueError("Не указано название схемы (schema_name).")
    
    print(f"[INFO] Основная таблица: {schema_name}.{table_name}")
    print(f"[INFO] Таблица брендов: {schema_name}.{categories_table_name}")
    
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
        
        # Создание таблицы брендов с уникальными значениями
        print(f"[INFO] Создание таблицы брендов '{schema_name}.{categories_table_name}' с уникальными categories.")
        
        # Формирование SQL-запроса
        create_brands_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {schema}.{categories_table} AS
            SELECT DISTINCT category_code
            FROM {main_schema}.{main_table}
            WHERE brand IS NOT NULL;
        """).format(
            schema=sql.Identifier(schema_name),
            categories_table=sql.Identifier(categories_table_name),
            main_table=sql.Identifier(table_name),
            main_schema=sql.Identifier(main_schema_name)
        )
        
        # Выполнение запроса
        cursor.execute(create_brands_table_query)
        conn.commit()
        print(f"[INFO] Таблица '{schema_name}.{categories_table_name}' успешно создана или обновлена.")
    
    except Exception as e:
        print(f"[ERROR] Произошла ошибка при создании таблицы брендов: {e}")
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
    
    print(f"[COMPLETE] Таблица '{schema_name}.{categories_table_name}' с уникальными брендами создана.")

def set_vars(**kwargs):
    test_var = Variable.get("test_var")
    Variable.set("test_var", int(test_var) + 1)

with DAG(
    'extract_tables',
    default_args=default_args,
    schedule_interval=None ,  # Установите нужное расписание
    catchup=False,
) as dag:

    get_vars = PythonOperator(
        task_id='get_vars',
        python_callable=get_vars,
    )

    create_categories_table = PythonOperator(
        task_id = 'create_categories_table',
        python_callable=create_categories_table
    )

    create_brands_table = PythonOperator(
        task_id = 'create_brands_table',
        python_callable=create_brands_table
    )
    
    set_vars = PythonOperator(
        task_id = 'set_vars',
        python_callable=set_vars
    )

    trigger_dag_ETL = TriggerDagRunOperator(
        task_id='trigger_dag_extract_tables',
        trigger_dag_id='ETL',  # ID DAG'а, который нужно запустить
        wait_for_completion=False,  # Установите True, если хотите ждать завершения DAG B
        poke_interval=30,  # Интервал проверки статуса DAG B (если wait_for_completion=True)
        conf={"message": "Запуск DAG ETL после успешного завершения DAG extract_tables"},
    )
    


    get_vars  >> create_brands_table >> create_categories_table >> set_vars >> trigger_dag_ETL
