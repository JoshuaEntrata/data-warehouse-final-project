from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

dag = DAG(
    'final-proj-dag',
    default_args={'start_date': days_ago(1)},
    schedule_interval='* */1 * * *', # subject to change pa
    catchup=False
)