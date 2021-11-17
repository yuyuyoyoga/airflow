from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

import yaml
import pandas as pd

# from utils import

DEFAULT_PARAMS = {}


class DataSourceOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            job_configs,
            job_name,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.__dict__.update(DEFAULT_PARAMS)
        self.__dict__.update(kwargs)
        self.job_configs = job_configs
        self.job_name = job_name

        # Load decryption resources
        # with open("config/cryptography_resources.yaml", "r") as f:
        #     crypto_resources = yaml.load(f, Loader=yaml.FullLoader)
        # for i, v in enumerate(crypto_resources[self.partner]['decryption']['key_name'], 1):
        #     self.decryption_config[f'key_{i}'] = trellis.keys(crypto_resources[self.partner]['decryption']['resource'])[
        #         v]
        # Load decryption resources

    def execute(self):
        df = self.query_data()

        df = self.decrypt_data(df)

        self.post_file_check(df)

        self.data_courier(df)

        self.output_data(df)

        self.output_data(df)

    def query_data(self):

        df_result = pd.DataFrame()

        for config in self.query_config:
            source = config.get('source', 'presto')
            query_path = config.get('query_path', '')
            primary_key = config.get('primary_key', None)
            query_params = config.get('query_params', {})
            qubole_env = config.get('qubole_env', 'prod')
            merge = config['merge']

            df = pd.read_csv(query_path)
            print(query_path)
            if merge:
                if merge['type'] == 'concat':
                    df_result = pd.concat([df_result, df])

                if merge['type'] == 'join':
                    df_result = df_result.merge(df, how=merge['direction'], on=merge['key'])
            else:
                df_result = df

            df_result = df_result[self.final_columns] if self.final_columns else df_result

        return df_result

    def decrypt_data(self, df):
        # d = Decrypt(df, self.decryption_config)
        # return d.decrypt_df()
        return df

    def post_file_check(self, df):
        return df

    def data_courier(self, df):
        pass

    def output_data(self, df):
        df.to_csv(f'output/{self.file_name}.csv')


class ChecksOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.__dict__.update(DEFAULT_PARAMS)
        self.__dict__.update(kwargs)

    def execute(self):
        return True


class FileTransferOperator(BaseOperator):
    DEFAULT_PARAMS = {
        'field_customization': None,
        'file_customization': None
    }

    @apply_defaults
    def __init__(
            self,
            file_configs,
            job_name,
            pii_fields,
            **kwargs
    ):
        super().__init__(**kwargs)
        self.__dict__.update(DEFAULT_PARAMS)
        self.__dict__.update(kwargs)
        self.__dict__.update(file_configs)
        self.job_name = job_name
        self.pii_fields = pii_fields

    def execute(self):

        df = self.get_file()

        df_no_pii = self.remove_piis(df.copy())

        df_pii = self.transform(df.copy())
        df_no_pii = self.transform(df_no_pii)

        df_pii = self.encrypt_file(df_pii)

        self.send_to_partner(df_pii)

        self.output_files(df_pii)
        self.output_files(df_no_pii)

    def get_file(self):
        return pd.read_csv(f'output/{self.job_name}.csv')

    def transform(self, df):
        if self.field_customization:
            pass

        if self.file_customization:
            pass
        return df

    def remove_piis(self, df):
        return df

    def encrypt_file(self, files):
        return files

    def to_json(self, df):
        return df

    def to_jsonl(self, df):
        return df

    def to_zip(self, df):
        return df

    def to_parquet(self, df):
        return df

    def send_to_partner(self, df):
        pass

    def send_via_sftp(self, files):
        pass

    def send_via_email(self, files):
        pass

    def send_via_s3(self, files):
        pass

    def send_via_google(self, files):
        pass

    def send_via_api(self, files):
        pass

    def output_files(self, file):
        # Output the raw value
        if isinstance(file, pd.DataFrame):
            file.to_csv(f'output/{self.file_name}')
        else:

            with open(f'output/{self.file_name}', 'w') as local_file:
                local_file.write(file)
