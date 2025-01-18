from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
import logging
import os
import chardet


# Список файлов и целевых таблиц
CSV_FILES_AND_TABLES = {
    "/opt/airflow/dags/deal_info.csv": "rd.deal_info_extra",
    "/opt/airflow/dags/product_info.csv": "rd.product_extra",
}

POSTGRES_CONN_ID = Variable.get("postgres_conn_id")
PG_LOGS_CONN_ID = Variable.get("postgres_logs_conn_id")

logging.basicConfig(
    filename="import_log.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# Функция для решения проблем с кодировкой
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']


# Функция импорта данных из CSV-файла в таблицу
def import_from_csv(file_path, table_name, **kwargs):
    try:
        logging.info(f"Начало импорта данных из файла {file_path} в таблицу {table_name}")
        if not os.path.exists(file_path):
            error_message = f"Файл {file_path} не найден. Импорт отменен."
            logging.error(error_message)
            raise FileNotFoundError(error_message)

        # Загрузка данных из CSV в DataFrame
        encoding = detect_encoding(file_path)
        df = pd.read_csv(file_path, sep=",", na_values=["NULL"], encoding=encoding)
        input_rows = len(df)

        # Подключение с помощью SQLAlchemy
        postgres_hook = PostgresHook(POSTGRES_CONN_ID)
        engine = postgres_hook.get_sqlalchemy_engine()

        # Подсчет строк до вставки
        with engine.connect() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name};")
            rows_before = result.scalar()

        # Вставка данных
        df.to_sql(table_name.split(".")[1], engine, schema=table_name.split(".")[0], if_exists="append", index=False)
        logging.info(f"Первые строки из {file_path}: {df.head()}")
        logging.info(f"Типы данных: {df.dtypes}")

        # Подсчет строк после вставки
        with engine.connect() as conn:
            result = conn.execute(f"SELECT COUNT(*) FROM {table_name};")
            rows_after = result.scalar()

        # Подсчет вставленных строк
        inserted_rows = rows_after - rows_before
        message = f"Данные из файла {os.path.basename(file_path)} успешно импортированы в таблицу {table_name}."
        logging.info(message)

        return {
            "file_path": file_path,
            "input_count": input_rows,
            "inserted_count": inserted_rows,
            "message_for_log": message,
        }
    except Exception as e:
        logging.error(f"Ошибка при импорте данных из {file_path}: {e}")
        raise


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
def update_etl_log_with_xcom(file_path, table_name, status, **kwargs):
    ti = kwargs['ti']
    log_id = ti.xcom_pull(task_ids=f'csv_to_dm_start_log_{table_name.replace(".", "_")}')
    
    hook = PostgresHook(postgres_conn_id=PG_LOGS_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        result = ti.xcom_pull(task_ids=f'import_from_{os.path.basename(file_path).replace(".", "_")}')
        
        if not result or not isinstance(result, dict):
            error_message = f"XCom data for task 'import_from_{os.path.basename(file_path)}' is invalid or missing."
            logging.error(error_message)
            raise ValueError(error_message)
                
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
    "retries": 0,
}

# Определение DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 0,
}

with DAG(
    "import_csv_to_multiple_tables_DAG",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="Импорт данных из CSV-файлов в таблицы",
) as dag:

    start = DummyOperator(
        task_id="start"
        )
    
    end = DummyOperator(
        task_id="end"
        )

    # Цикл для обработки каждого CSV и соответствующей таблицы
    for csv_file, target_table in CSV_FILES_AND_TABLES.items():
        # Логирование старта
        start_log_task = PythonOperator(
            task_id=f'csv_to_dm_start_log_{target_table.replace(".", "_")}',
            python_callable=start_etl_log,
            op_kwargs={'process': 'Import_CSV_to_DM', 'table_name': target_table, 'status': 'STARTED'}
        )

        # Импорт данных
        import_task = PythonOperator(
            task_id=f'import_from_{os.path.basename(csv_file).replace(".", "_")}',
            python_callable=import_from_csv,
            op_kwargs={"file_path": csv_file, "table_name": target_table},
        )

        # Логирование завершения
        end_log_task = PythonOperator(
            task_id=f'csv_to_dm_end_log_{target_table.replace(".", "_")}',
            python_callable=update_etl_log_with_xcom,
            op_kwargs={'file_path': csv_file, 'table_name': target_table, 'status': 'SUCCESS'}
        )

        # Установка зависимостей для текущей группы задач
        start >> start_log_task >> import_task >> end_log_task >> end
