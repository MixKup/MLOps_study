from airflow import DAG
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'mix_kup',
    'start_date': datetime(2023, 10, 1),
}

dag = DAG('spark_start', default_args=default_args, schedule_interval='@daily',)
ssh_hook = SSHHook(ssh_conn_id='Connect-to-spark', remote_host='158.160.2.234', cmd_timeout=None)

t1 = SSHOperator(
    ssh_hook = ssh_hook,
    task_id='run2_remote_spark',
    command='pip3 install boto3; pip3 install numpy; python3 /home/ubuntu/spark_valid_script.py',
    dag=dag)

t1
