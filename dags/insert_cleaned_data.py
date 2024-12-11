from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import logging

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
        dm_var = Variable.get("dm_var")
        logger.info(f"Текущий ключ: dm_var={dm_var}")
    except KeyError:
        dm_var = "1"
        Variable.set("dm_var", dm_var)
        logger.info(f"Переменная 'dm_var' не найдена. Установлено значение по умолчанию: {dm_var}")
    
    tables_json = Variable.get("tables_json", deserialize_json=True)
    ti.xcom_push(key='dm_var', value=dm_var)
    ti.xcom_push(key='tables_json', value=tables_json)
    logger.info(f"Получены переменные: dm_var={dm_var}, tables_json={tables_json}")

def generate_insert_sql(**kwargs):
    """
    Генерирует SQL-запрос для вставки данных из текущего датасета.
    Отправляет SQL-запрос в XCom.
    """
    ti = kwargs['ti']
    tables_json = ti.xcom_pull(key='tables_json', task_ids='get_vars')
    dm_var = ti.xcom_pull(key='dm_var', task_ids='get_vars')
    
    dataset_info = tables_json.get(dm_var)
    if not dataset_info:
        logger.info(f"Ключ '{dm_var}' не найден в tables_json. Завершение вставки.")
        ti.xcom_push(key='insert_sql', value="stop")
        return
    
    source_table = dataset_info.get('table_name')
    schema_stg = 'DDS-STG'
    schema_final = 'DDS'
    brand_category_mapping = 'brand_category_mapping_final'
    target_table = 'final_table'
    
    if not source_table:
        raise ValueError(f"Не указано 'table_name' для ключа '{dm_var}'.")
    
    insert_sql = f"""
        INSERT INTO "{schema_final}"."{target_table}" (
            event_time, 
            event_type, 
            product_id, 
            category_id, 
            category_code, 
            brand, 
            price, 
            user_id, 
            user_session
        )
        SELECT 
            st.event_time, 
            st.event_type, 
            st.product_id, 
            st.category_id, 
            st.category_code, 
            st.brand, 
            st.price, 
            st.user_id, 
            st.user_session
        FROM "{schema_stg}"."{source_table}" st
        JOIN "{schema_final}"."{brand_category_mapping}" bcm
          ON st.brand = bcm.brand
         AND st.category_code = bcm.category_code;
    """
    
    logger.info(f"Сгенерированный SQL-запрос для вставки: {insert_sql}")
    ti.xcom_push(key='insert_sql', value=insert_sql)

def set_vars(**kwargs):
    """
    Обновляет индекс (dm_var) для следующего датасета в Airflow Variables.
    """
    ti = kwargs['ti']
    dm_var = ti.xcom_pull(key='dm_var', task_ids='get_vars')
    tables_json = ti.xcom_pull(key='tables_json', task_ids='get_vars')
    
    # Проверяем, есть ли следующий ключ
    next_num = str(int(dm_var) + 1)
    if next_num in tables_json:
        Variable.set("dm_var", next_num)
        logger.info(f"Обновлён dm_var до {next_num}")
    else:
        logger.info("Все ключи обработаны. Рекурсивный запуск прекращен.")

with DAG(
    'insert_cleaned_data',
    default_args=default_args,
    schedule_interval=None,  # Установите нужное расписание, например, '0 0 * * *' для ежедневного запуска
    catchup=False,
) as dag:

    get_vars_task = PythonOperator(
        task_id='get_vars',
        python_callable=get_vars,
        provide_context=True,
    )

    generate_insert_sql_task = PythonOperator(
        task_id='generate_insert_sql',
        python_callable=generate_insert_sql,
        provide_context=True,
    )

    insert_cleaned_data_task = PostgresOperator(
        task_id='insert_cleaned_data',
        postgres_conn_id='database_cloud',  # Убедитесь, что Conn ID правильный
        sql="{{ task_instance.xcom_pull(task_ids='generate_insert_sql', key='insert_sql') }}",
    )

    set_vars_task = PythonOperator(
        task_id='set_vars',
        python_callable=set_vars,
        provide_context=True,
    )

    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_insert_cleaned_data',
        trigger_dag_id='insert_cleaned_data',  # ID текущего DAG для рекурсивного запуска
        wait_for_completion=False,  # Установите True, если хотите ждать завершения DAG
        poke_interval=30,  # Интервал проверки статуса DAG (если wait_for_completion=True)
        conf={"message": "Запуск следующего шага обработки таблиц"},
    )

    # Установка зависимостей между задачами
    get_vars_task >> generate_insert_sql_task >> insert_cleaned_data_task >> set_vars_task >> trigger_dag_task
