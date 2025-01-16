from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import logging
import os


CSV_FILE = "/opt/airflow/dags/dm_f101_round_f.csv"
POSTGRES_CONN_ID = "postgres-db"
PG_LOGS_CONN_ID = 'postgres-logs'

dm_name = Variable.get("dm_table_name", default_var="dm_f101_round_f_v2")


logging.basicConfig(
    filename="import_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# Функция импорта данных в витрину из CSV-файла
def import_from_csv():
    try:
        logging.info(f"Начало импорта данных из файла {CSV_FILE} в таблицу dm.dm_f101_round_f_v2")
        if not os.path.exists(CSV_FILE):
            logging.error(f"Файл {CSV_FILE} не найден. Импорт отменен.")
            raise FileNotFoundError(f"Файл {CSV_FILE} не найден")

        # Загрузка данных из CSV в DataFrame
        df = pd.read_csv(CSV_FILE, sep=";", na_values=["NULL"])
        input_rows = len(df)

        # Подключение с помощью SQLAlchemy
        postgres_hook = PostgresHook(POSTGRES_CONN_ID)
        engine = postgres_hook.get_sqlalchemy_engine()

        # Подсчет строк до вставки
        with engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM dm.dm_f101_round_f_v2;")
            rows_before = result.scalar()

        # Вставка данных
        df.to_sql("dm_f101_round_f_v2", engine, schema="dm", if_exists="append", index=False)
        logging.info(f"First rows from {CSV_FILE}: {df.head()}")
        logging.info(f"Data types: {df.dtypes}")

        # Подсчет строк после вставки
        with engine.connect() as conn:
            result = conn.execute("SELECT COUNT(*) FROM dm.dm_f101_round_f_v2;")
            rows_after = result.scalar()

        # Подсчет вставленных строк
        inserted_rows = rows_after - rows_before
        message = f"Данные из CSV-файла dm_f101_round_f.csv успешно импортированы в таблицу dm.dm_f101_round_f_v2."
        logging.info(message)

        return {
                "input_count": input_rows,
                "inserted_count": inserted_rows,
                "message_for_log": message,
            }
    except Exception as e:
        logging.error(f"Ошибка при импорте данных: {e}")
        raise


# Функция для очистки таблицы
def truncate_table(table_name, **kwargs):
    postgres_hook = PostgresHook(POSTGRES_CONN_ID)
    postgres_hook.run(f"TRUNCATE TABLE dm.{table_name} RESTART IDENTITY;")


# Функция для записи лога
def start_etl_log(process, table_name, status, **kwargs):
    
    hook = PostgresHook(postgres_conn_id=PG_LOGS_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()
    
    logging.info(f"TASK NAME csv_to_dm_start_log_{table_name}")

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
    ti = kwargs['ti']
    log_id = ti.xcom_pull(task_ids=f'csv_to_dm_start_log_{table_name}')
    
    hook = PostgresHook(postgres_conn_id=PG_LOGS_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        result = ti.xcom_pull(task_ids=f'import_from_csv_{dm_name}')
        
        if not result or not isinstance(result, dict):
            logging.error(f"XCom data for task 'import_from_csv_{dm_name}' is invalid or missing.")
            raise ValueError(f"Invalid or missing data from import_from_csv_{dm_name}")
                
        input_rows = result.get('input_count', 0)
        inserted_rows = result.get('inserted_count', 0)
        updated_rows = result.get('updated_count', 0)
        skipped_rows = result.get('skipped_count', 0)
        message = result.get('message_for_log', '')

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
    
    except Exception as e:
        error_message = str(e)
        logging.error(f"Ошибка при обновлении лога: {error_message}")
        cursor.execute(
            """
            UPDATE LOGS.etl_logs
            SET status='FAILED', end_time=NOW(), message=%s
            WHERE log_id=%s;
            """,
            (error_message, log_id)
        )
        conn.commit()
        raise  # Выбрасывается исключение для Airflow
    
    finally:
        cursor.close()


# Определение DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

with DAG(
    "import_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Импорт данных из CSV в таблицу dm.{dm_name}",
) as dag:
    
    start = DummyOperator(
        task_id="start"
        )
    
    # 1. Логирование старта ETL-процесса
    start_log_task = PythonOperator(
        task_id=f'csv_to_dm_start_log_{dm_name}',
        python_callable=start_etl_log,
        op_kwargs={'process': 'Import_to_DM', 'table_name': dm_name, 'status': 'STARTED'}
    )
    
    # 2. Предварительная очистка витрины
    truncate_task = PythonOperator(
        task_id=f'truncate_table_{dm_name}',
        python_callable=truncate_table,
        op_kwargs={'table_name': dm_name}
        )
    
    # 3. Импорт данных в витрину
    import_task = PythonOperator(
        task_id=f'import_from_csv_{dm_name}',
        python_callable=import_from_csv,
    )

    # 4. Логирование завершения ETL-процесса
    end_log_task = PythonOperator(
        task_id=f'dm_to_csv_end_log_{dm_name}',
        python_callable=update_etl_log_with_xcom,
        op_kwargs={'table_name': dm_name, 'status': 'SUCCESS'}
    )

    end = DummyOperator(
        task_id="end"
        )


# Определение зависимостей
    (
        start >> start_log_task  
        >> truncate_task >> import_task  
        >> end_log_task >> end
    )
