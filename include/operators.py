import sys

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from.reporting import *

def str_to_class(classname):
    return getattr(sys.modules[__name__], classname)


class ChecksOperator(BaseOperator):

    @apply_defaults
    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello {self.name}"
        print(context)
        print(message)
        return message


class ReadDataOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            job_configs,
            job_name,
            read_class_name,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.job_configs = job_configs
        self.job_name = job_name
        self.read_class_name = read_class_name

    def execute(self, context):
        self.run_date = context["logical_date"].date().strftime('%Y%m%d')

        r = str_to_class(self.read_class_name)(self.job_configs, self.job_name, self.run_date)
        r.read()


class SendFileOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            file_config,
            job_name,
            pii_fields,
            send_class_name,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.file_config = file_config
        self.job_name = job_name
        self.pii_fields = pii_fields
        self.send_class_name = send_class_name

    def execute(self, context):
        self.run_date = context["logical_date"].date().strftime('%Y%m%d')

        s = str_to_class(self.send_class_name)(self.file_config, self.job_name, self.run_date)

        s.send()
