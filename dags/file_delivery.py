from airflow import DAG
from datetime import datetime
from job_config.job_configs import configs
from include.operators import *

dag = DAG(
    dag_id="file_delivery_dag",
    schedule_interval=None,
    start_date=datetime(2018, 12, 31),
)


t1 = ChecksOperator(
    task_id='orig_agg_check'
)

for config in configs:
    job_name = config['job_name']
    job_configs = config['job_configs']
    file_configs = config['file_configs']
    t2 = DataSourceOperator(
        task_id=f'{job_name}',
        job_configs=job_configs,
        job_name=job_name,
        dag=dag
    )

    t1 >> t2

    pii_fields = file_configs['pii_fields']
    for file_config in file_configs['file_config']:
        file_name = file_config['file_name']
        t3 = FileTransferOperator(
            task_id=f'{file_name}',
            file_configs=file_config,
            pii_fields=pii_fields,
            job_name=job_name,
            dag=dag
        )

        t2 >> t3
