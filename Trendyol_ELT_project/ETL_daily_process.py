from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.Python import PythonOperator
from airflow.utils.dates import days_ago
from main import extract_from_page
from Transform import transform_from_csv, load_to_csv, load_to_db

default_args ={
    "owner": 'Emrehan Bilir',
    "email": 'emrehanbilir55@gmail.com',
    "Start_date": days_ago(0),
    "retries": 1,
    "retry_delay": timedelta(minutes=50),
}

#DAG Definition
dag = DAG("ELT_Data_Pipeline",
    Descripiton= 'Apache Airflow DAG for ELT process',
    default_args=default_args,
    schedule= timedelta(minutes=50),
)

#TASKs Definitons
extract_task = PythonOperator(
    task_id='extract_phase',
    python_callable=extract_from_page,
    dag=DAG,
)

transform_task = PythonOperator(
    task_id='transform_phase',
    python_callable=transform_from_csv,
    dag=DAG,
)

load_csv_task = PythonOperator(
    task_id='load_csv_phase',
    python_callable=load_to_csv,
    dag=DAG,
)

load_db_task = PythonOperator(
    task_id='load_db_phase',
    python_callable=load_to_db,
    dag=DAG,
)


# Tasks Dependencies
extract_task >> transform_task >> [load_csv_task,load_db_task]