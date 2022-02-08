from dag_factory.utils import *


def create_dag(dag_id, config_var_name, read_class_name, send_class_name, schedule):
    """
    this is the main factory method which is used for creating dynamic dags

    Args:
        dag_id ([string]): dag name
        config_var ([string], optional): configuration name. Defaults to None.
        originations_data_contract (bool, optional): [description]. Defaults to True.
        servicing_data_contract (bool, optional): [description]. Defaults to True.
        context_run (bool, optional): [description]. Defaults to False.
        schedule ([string], optional): crontab expression. Defaults to None.

    Raises:
        RuntimeError: for now if more than 1 originations data contract table is given in admin->variables, it raises error

    Returns:
        DAG: dag object
    """

    # get config from airflow UI Variables
    config_dict = get_airflow_variable(config_var_name)
    print(config_dict)

    dag = build_dag(dag_id, 'REPORTING DAG', schedule, None, enable_pagerduty=True)

    main_reporting_process = MainReportingProcesses(config_dict, dag, read_class_name, send_class_name)

    main_reporting_process.reporting_build()

    return dag


reporting_configs = load_configs()

for reporting_config in reporting_configs:
    dag_name = reporting_config['dag_name']
    config_var_name = reporting_config['config']
    read_class_name = reporting_config.get('read_class_name', 'standard_read_data')
    send_class_name = reporting_config.get('send_class_name', 'standard_send_data')
    schedule = reporting_config.get('schedule', None)

    globals()[dag_name] = create_dag(dag_name,
                                     config_var_name=config_var_name,
                                     read_class_name=read_class_name,
                                     send_class_name=send_class_name,
                                     schedule=schedule)
