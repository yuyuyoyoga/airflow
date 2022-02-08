from abc import ABC, abstractmethod
import pandas as pd


class ReadDataBase(ABC):
    def __init__(
            self,
            job_configs,
            job_name,
            **kwargs
    ):
        self.__dict__.update(kwargs)
        self.job_configs = job_configs
        self.job_name = job_name
        super().__init__()

    @abstractmethod
    def read(self):
        raise NotImplementedError()

    @abstractmethod
    def query_data(self):
        return pd.DataFrame()

    @abstractmethod
    def decrypt_data(self, df):
        return df

    def data_courier(self, df):
        raise NotImplementedError()

    @abstractmethod
    def output_data(self, df):
        raise NotImplementedError()


class SendDataBase(ABC):

    def __init__(
            self,
            file_config,
            job_name,
            **kwargs
    ):
        self.file_config = file_config
        self.job_name = job_name
        self.__dict__.update(kwargs)

        super().__init__()

    def send(self):
        pass

    @abstractmethod
    def read_data(self):
        return pd.DataFrame()

    def remove_piis(self, df):
        return df

    def field_transform(self, df):
        return df

    def file_transform(self, df):
        return df

    @abstractmethod
    def encrypt_file(self, files):
        return files

    @abstractmethod
    def send_to_partner(self, df):
        raise NotImplementedError()

    @abstractmethod
    def output_files(self, file):
        raise NotImplementedError()
