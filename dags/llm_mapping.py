from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging
import psycopg2
from psycopg2 import sql
import re
from g4f import Client
from g4f.Provider import Blackbox

# Настройка логирования
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

default_args = {
    'owner': 'artzsh',
    'start_date': datetime(2024, 4, 27),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

def get_next_batch(**kwargs):
    """
    Функция для получения следующего батча брендов из таблицы 'DDS.unique_brands'.
    Возвращает словарь с ключами 'batch' и 'next_index'.
    """
    current_index = int(Variable.get("brand_batch_index", default_var=0))
    batch_size = 25
    next_index = current_index + batch_size
    base_schema = 'DDS'

    conn_id = 'database_cloud'  # Замените на ваше Conn Id
    connection = BaseHook.get_connection(conn_id)
    postgres_conn_params = {
        'host': connection.host,
        'port': connection.port,
        'dbname': connection.schema,
        'user': connection.login,
        'password': connection.password,
        'client_encoding': 'UTF8'
    }

    try:
        conn = psycopg2.connect(**postgres_conn_params)
        cursor = conn.cursor()
        cursor.execute("SET client_encoding TO 'UTF8';")

        query = sql.SQL('''SELECT brand FROM "DDS".unique_brands ORDER BY brand LIMIT {limit} OFFSET {offset}''').format(
            limit=sql.Literal(batch_size),
            offset=sql.Literal(current_index),
            # schema=sql.Literal(base_schema)
        )
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            logger.info("Все бренды обработаны. Завершение работы DAG.")
            return {'batch': [], 'next_index': next_index}

        brands = []
        for row in results:
            brand = row[0]
            try:
                brand = brand.encode('utf-8').decode('utf-8')
            except UnicodeDecodeError:
                brand = brand.encode('utf-8', errors='replace').decode('utf-8')
                logger.warning(f"Некорректный символ в бренде, заменён: {brand}")
            brands.append(brand)
        
        logger.info(f"Извлечено брендов: {brands}")

        return {'batch': brands, 'next_index': next_index}

    except Exception as e:
        logger.error(f"Ошибка при извлечении брендов: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def update_batch_index(next_index):
    """
    Функция для обновления индекса батча в Airflow Variables.
    """
    Variable.set("brand_batch_index", next_index)
    logger.info(f"Обновлён brand_batch_index до {next_index}")

def call_llm(batch):
    """
    Функция для вызова LLM и получения SQL-кода.
    """
    if not batch:
        logger.info("Пустой батч. Ничего не делать.")
        return ""

    brands_str = ',\n '.join([f"'{brand}'" for brand in batch])

    prompt = f'''
You are an AI assistant that uses search queries to perform actions.

You are tasked with mapping a list of brands to predefined categories based on what each brand sells.

Category codes: apparel, accessories, appliances, auto, computers, construction, country_yard, electronics, furniture, kids, medicine, sport, stationery.

Brands: {brands_str}

For each brand, perform the following steps:
1. Perform an internet search with the query "what is the category of (brand) products".
2. Based on the search results, determine the most appropriate categories (there can be multiple categories for one brand) for the brand from the given category codes.
3. If you can't find anything on the web, just assign some categories that fit.
4. Collect the brand and its assigned categories.

After processing all brands, generate an SQL INSERT statement in the following format:

INSERT INTO "DDS".brand_category_mapping (brand, category_index) VALUES
    ('valberg', 'electronics'),
    ('valentino', 'apparel'),
    ('valentino', 'accessories'),
    ... ;
'''

    logger.info("Отправка запроса к LLM.")

    try:
        client = Client(provider=Blackbox)
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )
        llm_response = response.choices[0].message.content
        logger.info(f"Получен ответ от LLM: {llm_response}")
        return llm_response

    except Exception as e:
        logger.error(f"Ошибка при вызове LLM: {e}")
        raise

def call_llm_wrapper(**kwargs):
    """
    Обёртка для функции call_llm.
    Извлекает 'batch' из XCom и передаёт его в call_llm.
    """
    ti = kwargs['ti']
    return_value = ti.xcom_pull(task_ids='get_next_batch', key='return_value')
    logger.info(f"Извлечён return_value: {return_value}")
    if return_value and 'batch' in return_value:
        batch = return_value['batch']
    else:
        batch = []
    return call_llm(batch)

def extract_sql(llm_response, **kwargs):
    """
    Функция для извлечения SQL-кода из ответа LLM.
    """
    if not llm_response:
        logger.info("Пустой ответ от LLM. Ничего не делать.")
        return ""

    # Ищем текст между тройными обратными кавычками с указанием языка sql
    match = re.search(r'```sql\n(.*?)```', llm_response, re.DOTALL)
    if match:
        sql_code = match.group(1).strip()
        logger.info(f"Извлечён SQL-код:\n{sql_code}")
        return sql_code
    else:
        logger.error("Не удалось найти SQL-код в ответе от LLM.")
        raise ValueError("SQL-код не найден в ответе LLM.")

def extract_sql_wrapper(**kwargs):
    """
    Обёртка для функции extract_sql.
    Извлекает 'llm_response' из XCom и передаёт его в extract_sql.
    """
    ti = kwargs['ti']
    llm_response = ti.xcom_pull(task_ids='call_llm', key='return_value')
    logger.info(f"Извлечён llm_response: {llm_response}")
    return extract_sql(llm_response)

def execute_sql(sql_code, **kwargs):
    """
    Функция для выполнения SQL-кода в базе данных.
    """
    if not sql_code:
        logger.info("Пустой SQL-код. Ничего не делать.")
        return

    conn_id = 'database_cloud'  # Замените на ваше Conn Id
    connection = BaseHook.get_connection(conn_id)
    postgres_conn_params = {
        'host': connection.host,
        'port': connection.port,
        'dbname': connection.schema,
        'user': connection.login,
        'password': connection.password,
        'client_encoding': 'UTF8'
    }

    try:
        conn = psycopg2.connect(**postgres_conn_params)
        cursor = conn.cursor()
        cursor.execute("SET client_encoding TO 'UTF8';")

        logger.info(f"Выполнение SQL-кода:\n{sql_code}")
        cursor.execute(sql_code)
        conn.commit()
        logger.info("SQL-код успешно выполнен.")

    except Exception as e:
        logger.error(f"Ошибка при выполнении SQL-кода: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def execute_sql_wrapper(**kwargs):
    """
    Обёртка для функции execute_sql.
    Извлекает 'sql_code' из XCom и передаёт его в execute_sql.
    """
    ti = kwargs['ti']
    sql_code = ti.xcom_pull(task_ids='extract_sql', key='return_value')
    logger.info(f"Извлечён sql_code: {sql_code}")
    return execute_sql(sql_code)

def update_batch_index_wrapper(**kwargs):
    """
    Обёртка для функции update_batch_index.
    Извлекает 'next_index' из XCom и передаёт его в update_batch_index.
    """
    ti = kwargs['ti']
    return_value = ti.xcom_pull(task_ids='get_next_batch', key='return_value')
    logger.info(f"Извлечён return_value для обновления индекса: {return_value}")
    if return_value and 'next_index' in return_value:
        next_index = return_value['next_index']
    else:
        next_index = 0
    return update_batch_index(next_index)

with DAG(
    'brand_category_mapping',
    default_args=default_args,
    schedule_interval=None,  # DAG запускается вручную или триггерится самим
    catchup=False,
    max_active_runs=1,  # Ограничение на одновременные запуски DAG
) as dag:

    # Задача получения следующего батча брендов
    get_batch = PythonOperator(
        task_id='get_next_batch',
        python_callable=get_next_batch,
        provide_context=True,
    )

    # Задача вызова LLM
    llm_task = PythonOperator(
        task_id='call_llm',
        python_callable=call_llm_wrapper,
        provide_context=True,
    )

    # Задача извлечения SQL-кода из ответа LLM
    extract_sql_task = PythonOperator(
        task_id='extract_sql',
        python_callable=extract_sql_wrapper,
        provide_context=True,
    )

    # Задача выполнения SQL-кода
    execute_sql_task = PythonOperator(
        task_id='execute_sql',
        python_callable=execute_sql_wrapper,
        provide_context=True,
    )

    # Задача обновления индекса батча
    update_index = PythonOperator(
        task_id='update_batch_index',
        python_callable=update_batch_index_wrapper,
        provide_context=True,
    )

    trigger_dag_llm_mapping = TriggerDagRunOperator(
        task_id='trigger_dag_llm_mapping',
        trigger_dag_id='brand_category_mapping',  # ID DAG'а, который нужно запустить
        wait_for_completion=False,  # Установите True, если хотите ждать завершения DAG B
        poke_interval=30,  # Интервал проверки статуса DAG B (если wait_for_completion=True)
        conf={"message": "Запуск DAG extract_tables после успешного завершения DAG ETL"},
    )   

    # Установка зависимостей между задачами
    get_batch >> llm_task >> extract_sql_task >> execute_sql_task >> update_index >> trigger_dag_llm_mapping
