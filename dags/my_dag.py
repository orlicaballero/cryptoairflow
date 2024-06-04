from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/dags')
from api2 import main_function

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'update_criptomonedas',
    default_args=default_args,
    description='Actualiza la tabla de criptomonedas en Redshift diariamente',
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='run_api2_script',
    python_callable=main_function,
    dag=dag,
)

t1
