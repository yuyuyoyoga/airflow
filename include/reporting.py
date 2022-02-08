import pandas as pd

from .reporting_base import ReadDataBase, SendDataBase


class read_data_standard(ReadDataBase):
    def __init__(
            self,
            job_configs,
            job_name,
            run_date,
            **kwargs
    ):
        self.__dict__.update(kwargs)
        self.run_date = run_date
        super().__init__(job_configs, job_name, )

    def read(self):
        df = self.query_data()
        df = self.decrypt_data(df)
        self.data_courier(df)
        self.output_data(df)

    def query_data(self):
        return pd.DataFrame()

    def decrypt_data(self, df):
        return df

    def data_courier(self, df):
        pass

    def output_data(self, df):
        df.to_csv(f'logs/{self.job_name}.csv')


class send_data_standard(SendDataBase):

    def __init__(
            self,
            file_config,
            job_name,
            run_date,
            **kwargs
    ):
        self.__dict__.update(kwargs)
        self.run_date = run_date

        super().__init__(file_config, job_name)
        self.__dict__.update(file_config)

    def send(self):
        df = self.read_data()
        df = self.field_transform(df)
        df = self.file_transform(df)
        df = self.encrypt_file(df)
        self.send_to_partner(df)
        self.output_files(df)

    def read_data(self):
        return pd.read_csv(f'logs/{self.job_name}.csv')

    def remove_piis(self, df):
        return df

    def field_transform(self, df):
        return df

    def file_transform(self, df):
        return df

    def encrypt_file(self, file):
        return file

    def send_to_partner(self, file):
        pass

    def output_files(self, file):
        print(self.file_name.format(file_date=self.run_date))


class send_data_cof(send_data_standard):

    def __init__(
            self,
            file_config,
            job_name,
            run_date,
            **kwargs
    ):

        self.__dict__.update(kwargs)
        super().__init__(file_config, job_name, run_date)
        self.__dict__.update(file_config)

    def send(self):

        df = self.read_data()
        df = self.field_transform(df)
        df = self.file_transform(df)
        df = self.encrypt_file(df)
        df_parquet = self.to_parquet(df)
        self.send_to_partner(df_parquet)
        self.output_files(df_parquet)

    def to_parquet(self, df):
        print('Transfer file into parquet format')
        return df
