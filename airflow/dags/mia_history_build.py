from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
from gather_all_commits import list_commits


default_args = {
    'owner': 'Hengel',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2022, 11, 21, hour=0, tz="UTC"),
    'email': ['hengelaura@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    dag_id="mia_github_review",
    description = 'Minneapolis Institute of Art official github crawl',
    #schedule_interval = '0 12 * * *',
    start_date = datetime(2017,3,20), catchup = False)

t1 = PythonOperator(task_id='list-commits', python_callable=list_commits, dag=dag)