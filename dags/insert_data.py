from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.configuration import conf
from airflow.models import Variable
import chardet

import logging
import time

import pandas
from datetime import datetime

PG_CONN_ID = 'postgres-db'
PG_LOGS_CONN_ID = 'postgres-logs'

PATH = Variable.get("my_path")
conf.set("core", "template_searchpath", PATH)


def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']


def insert_data(table_name):
    file_path = PATH + f"{table_name}.csv"  # код для исправления ошибки с кодировкой в одной из таблиц и возможных аналогичных ошибок
    encoding = detect_encoding(file_path)
    print(f"Detected encoding for {file_path}: {encoding}")
    
    df = pandas.read_csv(file_path, delimiter=";", encoding=encoding)
    postgres_hook = PostgresHook(PG_CONN_ID)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(table_name, engine, schema="stage", if_exists="append", index=False)
    logging.info(f"First rows from {file_path}: {df.head()}")
    logging.info(f"Data types: {df.dtypes}")


default_args = {
    "owner" : "as",
    "start_date" : datetime(2024, 2, 25),
    "retries": 0
}

# Функция для записи лога
def start_etl_log(process, table_name, status, **kwargs):
    hook = PostgresHook(postgres_conn_id=PG_LOGS_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    logging.info(f"TASK NAME sql_start_log_{table_name}")

    cursor.execute(
        """
            INSERT INTO logs.etl_logs (process, table_name, start_time, status, message)
            VALUES (%s, %s, NOW(), %s, 'Data load started')
            RETURNING log_id;
        """,
        (process, table_name, status)
    )
    log_id = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    return log_id



# Функция для обновления лога с получением данных из Xcom
def update_etl_log_with_xcom(table_name, status, **kwargs):
    time.sleep(5)

    ti = kwargs['ti']
    log_id = ti.xcom_pull(task_ids=f'sql_start_log_{table_name}')
    result = ti.xcom_pull(task_ids=f'copy_and_trans_{table_name}')
    
    if not result:
        raise ValueError(f"No result received from copy_and_trans_{table_name}")
    
    input_rows = result.get('input_count', 0)
    inserted_rows = result.get('inserted_count', 0)
    updated_rows = result.get('updated_count', 0)
    skipped_rows = result.get('skipped_count', 0)
    message = result.get('message_for_log', '')

    hook = PostgresHook(postgres_conn_id=PG_LOGS_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute(
        """
        UPDATE LOGS.etl_logs
        SET status=%s, end_time=NOW(), message=%s, input_rows=%s, inserted_rows=%s, updated_rows=%s, skipped_rows=%s
        WHERE log_id=%s;
        """,
        (
            status, message,
            input_rows, inserted_rows, updated_rows, skipped_rows,
            log_id
        )
    )
    conn.commit()
    cursor.close()


# Функция для переноса данных из схемы stage в схему DS и трансформации данных
def copy_and_transform(table_name, **kwargs):
    
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID) 
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    try:
        sql = f"SELECT input_count, inserted_count, updated_count, skipped_count, message_for_log FROM PROCESS_{table_name}();"
        cursor.execute(sql)
        result = cursor.fetchone()
        logging.info(f"Result from PROCESS_{table_name}: {result}")
        if result:
            (input_count, inserted_count, updated_count, skipped_count, message_for_log) = result
        else:
            input_count = inserted_count = updated_count = skipped_count = 0
            message_for_log = ''
        
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()
    
    return {
            'input_count': input_count,
            'inserted_count': inserted_count,
            'updated_count': updated_count,
            'skipped_count': skipped_count,
            'message_for_log': message_for_log
        }


# Функция для очистки таблиц
def truncate_table(table_name, **kwargs):
    hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE stage.{table_name} RESTART IDENTITY;")
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


tables = [
    'ft_balance_f', 
    'ft_posting_f', 
    'md_account_d', 
    'md_currency_d', 
    'md_exchange_rate_d', 
    'md_ledger_account_s'
]


with DAG(
    "insert_data_into_DS",
    default_args = default_args,
    description="Загрузка данных в схему stage, трансформация и сохранение в схему DS",
    catchup=False,
    template_searchpath = [PATH],
    schedule="0 0 * * *"
) as dag:
    
    start = DummyOperator(
        task_id="start"
    )

    split_start_log_and_download = DummyOperator(
        task_id='split_start_log_and_download'
    )

    split_download_and_copy = DummyOperator(
        task_id='split_download_and_copy'
    )

    split_copy_and_truncate = DummyOperator(
        task_id='split_copy_and_truncate'
    )

    split_truncate_and_end_log = DummyOperator(
        task_id='split_truncate_and_end_log'
    )

    end = DummyOperator(
        task_id="end"
    )

    # Создание задач с использованием цикла для сокращения объема кода
    start_log_tasks = []
    download_tasks = []
    copy_tasks = []
    truncate_tasks = []
    end_log_tasks = []

    for table in tables:
        # 1. Логирование старта ETL-процесса
        start_log_task = PythonOperator(
            task_id=f'sql_start_log_{table}',
            python_callable=start_etl_log,
            op_kwargs={'process': 'ETL_DS_Load', 'table_name': table, 'status': 'STARTED'}
        )
        start_log_tasks.append(start_log_task)

        # 2. Загрузка исходных данных в схему stage
        download_task = PythonOperator(
            task_id=f'download_to_stage_{table}',
            python_callable=insert_data,
            op_kwargs={'table_name': table}
        )
        download_tasks.append(download_task)

        # 3. Копирование и трансформация данных
        copy_task = PythonOperator(
            task_id=f'copy_and_trans_{table}',
            python_callable=copy_and_transform,
            op_kwargs={'table_name': table}
        )
        copy_tasks.append(copy_task)
         
        # 4. Очистка промежуточных таблиц в схеме stage
        truncate_task = PythonOperator(
            task_id=f'truncate_table_task_{table}',
            python_callable=truncate_table,
            op_kwargs={'table_name': table}
        )
        truncate_tasks.append(truncate_task)

        # 5. Логирование завершения ETL-процесса
        end_log_task = PythonOperator(
            task_id=f'sql_end_log_{table}',
            python_callable=update_etl_log_with_xcom,
            op_kwargs={'table_name': table, 'status': 'SUCCESS'}
        )
        end_log_tasks.append(end_log_task)

    # Определение зависимостей
    (
        start >> start_log_tasks >> split_start_log_and_download >>
        download_tasks >> split_download_and_copy >> 
        copy_tasks >> split_copy_and_truncate >>  
        truncate_tasks >> split_truncate_and_end_log >> 
        end_log_tasks >> end
    )
