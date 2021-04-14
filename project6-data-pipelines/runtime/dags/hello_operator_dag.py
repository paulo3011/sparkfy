from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from custom_operator.hello_operator import HelloOperator

def print_hello():
 return 'Hello Wolrd'

dag = DAG('hello_operator', description='Hello world example', schedule_interval='0 12 * * *', start_date=datetime(2017, 3, 20), catchup=False)

dymmy = PythonOperator(task_id='hello_operator_task', python_callable=print_hello, dag=dag)

hello = HelloOperator(task_id='test_operator_task_id',name="test_operator_run")
#
