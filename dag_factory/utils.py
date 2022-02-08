import json
from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

from include.operators import *


def build_dag(dag_id, dag_description, schedule, timezone=None, enable_pagerduty=True):
    default_args = {
        'owner': 'Amount Data',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=1)
    }

    dag = DAG(
        dag_id,
        default_args=default_args,
        description=dag_description,
        schedule_interval=schedule,
        start_date=datetime(2021, 1, 1, tzinfo=pendulum.timezone(timezone) if timezone else None),
        catchup=False,
        default_view='graph',
        orientation='LR'
    )

    return dag

def get_airflow_variable(config_var_name):
    """
    Read the content of Ariflow variable.
    :return:
    """
    # get config from airflow UI Variables
    config_dict = Variable.get(config_var_name, default_var={}, deserialize_json=True)
    return config_dict


def load_configs():
    path = 'dags/job_config/job_configs.json'
    with open(path, 'r') as r:
        configs = json.load(r)

    return configs


class MainReportingProcesses:
    """
    Initialize configs, these configs come from airflow admin->variables
    Args:
        config_dict ([type]): [description]
        dag (Object): dag object passed when this class is initiated
    """

    def __init__(self, config_dict, dag, read_class_name, send_class_name) -> None:
        self.config_dict = config_dict
        self.dag = dag
        self.read_class_name = read_class_name
        self.send_class_name = send_class_name

    def reporting_build(self):

        orig_agg_check = ChecksOperator(task_id='orig_agg_check', name='orig_check')
        serv_agg_check = ChecksOperator(task_id='serv_agg_check', name='serv_check')

        all_done = DummyOperator(task_id='all_done')



        for config in self.config_dict:
            job_name = config['job_name']
            job_type = config['job_type']
            job_configs = config['job_config']
            file_configs = config['file_configs']

            # dummy
            file_dq_check = DummyOperator(task_id=f'{job_name}_dq_check')

            read_data = ReadDataOperator(
                task_id=f'{job_name}',
                job_configs=job_configs,
                job_name=job_name,
                read_class_name=self.read_class_name,
                dag=self.dag
            )
            if job_type == 'origination':
                orig_agg_check >> read_data
            elif job_type == 'servicing':
                serv_agg_check >> read_data

            pii_fields = file_configs.get('pii_fields', [])

            with self.dag as dag:
                with TaskGroup(group_id=f'{job_name}_group') as file_group:
                    for file_config in file_configs['file_config']:
                        config_name = file_config['config_name']
                        send_file = SendFileOperator(
                            task_id=f'{config_name}',
                            file_config=file_config,
                            pii_fields=pii_fields,
                            job_name=job_name,
                            send_class_name=self.send_class_name,
                            dag=self.dag
                        )
            
            health_check = DummyOperator(task_id=f'healthcheck_{job_name}')
            
            read_data >> file_dq_check >> file_group >> health_check >> all_done
