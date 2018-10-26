from airflow import DAG
from airflow.contrib.operators.azure_container_instances_operator import AzureContainerInstancesOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 11, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'aci_example',
    default_args=default_args,
    schedule_interval=timedelta(1)
)

t1 = AzureContainerInstancesOperator(
    'azure_container_instances_default',
    None,                               # Registry connection user
    'resource-group',
    'aci-test-{{ ds }}',
    'hello-world',
    'WestUS2',
    {},                                 # Environment Variables
    [],                                 # Volumes
    memory_in_gb=4.0,
    cpu=1.0,
    task_id='start_container',
    dag=dag
)
