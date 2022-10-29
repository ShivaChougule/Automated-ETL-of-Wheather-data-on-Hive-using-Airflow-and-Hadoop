from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator

default_arguments={'owner':'Shivaji','start_date':days_ago(1)}

with DAG('wheather_data',
	schedule_interval = @hourly,
	catchup=False,
	default_args=default_arguments
)as dag:
	pull=BashOperator(task_id='datapull',bash_command='python /home/talentum/Desktop/airflow-tutorial/dags/datapull.py')

	ingest=BashOperator(task_id='data_ingest',bash_command='hive -f /home/talentum/Desktop/airflow-tutorial/dags/hivetemp.hive')
	
	pull >> ingest