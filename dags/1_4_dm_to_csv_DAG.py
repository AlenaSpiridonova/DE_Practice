from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import os
import logging


CSV_FILE = Variable.get("dm_csv_file", "/opt/airflow/dags/dm_f101_round_f.csv")
POSTGRES_CONN_ID = Variable.get("postgres_conn_id", "postgres-db")
PG_LOGS_CONN_ID = Variable.get("postgres_logs_conn_id", "postgres-logs")

table_name = Variable.get("dm_table_name", default_var="dm_f101_round_f")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# Функция экспорта данных в CSV-файл
def export_to_csv():
    try:
        logging.info("Начало экспорта данных из витрины dm.dm_f101_round_f")

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        query = f"SELECT * FROM dm.{table_name}"
        df = hook.get_pandas_df(sql=query)

        input_rows = len(df)

        df.to_csv(CSV_FILE, index=False, sep=";", na_rep="NULL")

        if not os.path.exists(CSV_FILE):
            raise FileNotFoundError(f"CSV-файл {CSV_FILE} не был создан.")

        message = f"Данные из витрины dm.{table_name} успешно экспортированы в CSV-файл {CSV_FILE}."
        logging.info(message)

        return {
            "input_count": input_rows,
            "message_for_log": message,
        }

    except Exception as e:
        logging.error(f"Ошибка при экспорте данных: {e}")
        raise

# Функция для записи лога
def start_etl_log(process, table_name, status, **kwargs):
    hook = PostgresHook(postgres_conn_id=PG_LOGS_CONN_ID)

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            logging.info(f"TASK NAME dm_to_csv_start_log_{table_name}")

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
    
    return log_id

# Функция для обновления лога с получением данных из Xcom
def update_etl_log_with_xcom(table_name, status, **kwargs):
    ti = kwargs['ti']
    log_id = ti.xcom_pull(task_ids=f'dm_to_csv_start_log_{table_name}')

    if not log_id:
        raise ValueError(f"Log ID for task 'dm_to_csv_start_log_{table_name}' is missing.")

    hook = PostgresHook(postgres_conn_id=PG_LOGS_CONN_ID)

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            try:
                result = ti.xcom_pull(task_ids=f'export_csv_{table_name}')

                if not result or not isinstance(result, dict):
                    raise ValueError(f"Invalid or missing data from export_csv_{table_name}")

                input_rows = result.get('input_count', 0)
                message = result.get('message_for_log', '')

                cursor.execute(
                    """
                    UPDATE logs.etl_logs
                    SET status=%s, end_time=NOW(), message=%s, input_rows=%s
                    WHERE log_id=%s;
                    """,
                    (status, message, input_rows, log_id)
                )
                conn.commit()

            except Exception as e:
                error_message = f"Ошибка при обновлении лога: {e}"
                logging.error(error_message)
                cursor.execute(
                    """
                    UPDATE logs.etl_logs
                    SET status='FAILED', end_time=NOW(), message=%s
                    WHERE log_id=%s;
                    """,
                    (error_message, log_id)
                )
                conn.commit()
                raise

# Параметры по умолчанию для DAG
default_args = {
    "owner": "as",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 15),
    "retries": 0,
}

# Определение DAG
with DAG(
    "export_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Экспорт данных из витрины dm.{table_name} в CSV-файл", 
) as dag:

    start = DummyOperator(task_id="start")

    # 1. Логирование старта ETL-процесса
    start_log_task = PythonOperator(
        task_id=f'dm_to_csv_start_log_{table_name}',
        python_callable=start_etl_log,
        op_kwargs={'process': 'Export_to_CSV', 'table_name': table_name, 'status': 'STARTED'},
    )

    # 2. Экспорт данных в CSV
    export_task = PythonOperator(
        task_id=f'export_csv_{table_name}',
        python_callable=export_to_csv,
    )

    # 3. Логирование завершения ETL-процесса
    end_log_task = PythonOperator(
        task_id=f'dm_to_csv_end_log_{table_name}',
        python_callable=update_etl_log_with_xcom,
        op_kwargs={'table_name': table_name, 'status': 'SUCCESS'},
    )

    end = DummyOperator(task_id="end")

    # Определение зависимостей
    (
        start
        >> start_log_task
        >> export_task
        >> end_log_task
        >> end
    )
