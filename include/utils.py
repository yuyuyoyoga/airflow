import pandas as pd
import numpy as np
import hashlib
from base64 import b64decode
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from functools import lru_cache
from typing import List, Tuple, Callable
from concurrent.futures import TimeoutError
from pebble import ProcessPool, ProcessExpired

from pytz import timezone
from datetime import datetime

DEFAULT_PARAMS = {
    'encoding': 'utf-8',
    'filter_config': {},
    'splits': 4,
    'pool_size': 4,
    'multiprocessing': True,
    'pd_alert': False,
    'ruby_clean': []
}

def duration(function: Callable) -> Callable:
    def new_function(*args, **kwargs):
        central = timezone("US/Central")
        start_time = datetime.now(central)
        print('-' * 67)
        print("Running {} at {} CST".format(function.__name__, start_time))
        result = function(*args, **kwargs)
        print(
            "{} completed at {} CST, runtime of {}".format(
                function.__name__,
                datetime.now(central),
                datetime.now(central) -
                start_time))
        print('-' * 67)
        print('-' * 67)
        return result

    return new_function


import pandas as pd
import numpy as np
import hashlib
from base64 import b64decode
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from functools import lru_cache
from typing import List, Tuple
from concurrent.futures import TimeoutError
from pebble import ProcessPool, ProcessExpired

DEFAULT_PARAMS = {
    'encoding': 'utf-8',
    'filter_config': {},
    'splits': 4,
    'pool_size': 4,
    'multiprocessing': True,
    'pd_alert': False,
    'ruby_clean': []
}


class Decrypt():

    def __init__(
            self,
            df: pd.DataFrame,
            decryption_config,
            **kwargs) -> None:

        self.__dict__.update(DEFAULT_PARAMS)
        self.__dict__.update(kwargs)

        self.__dict__.update(decryption_config)

        self.df = df

    @duration
    def decrypt_df(self) -> pd.DataFrame:
        # Global variable for decrypt_fields function to access dataframe by index
        global global_list_dfs
        decryption_failed = False

        if self.multiprocessing:
            # Split df into parts, then pass the index to multiprocessing pool for decryption
            global_list_dfs = np.array_split(self.df, self.splits)
            del self.df

            # Start decryption
            result = []
            with ProcessPool(max_workers=self.pool_size) as pool:
                future = pool.map(self.decrypt_fields,
                                  range(len(global_list_dfs)),
                                  timeout=1800)
                r = future.result()
                while True:
                    try:
                        result.append(next(r))
                    except StopIteration:
                        break
                    except TimeoutError as error:
                        print("Decryption failed due to worker timeout: %s" % str(error.args))
                        decryption_failed = True
                    except ProcessExpired as error:
                        print("%s. Exit code: %d" % (error, error.exitcode))
                        decryption_failed = True
                    except Exception as error:
                        print("function raised %s" % error)
                        print(error.traceback)  # Python's traceback of remote process
                        raise
            del global_list_dfs
            del r

            if not decryption_failed:
                # Join the parts together
                result = pd.concat(result, ignore_index=True)
        else:
            # Regular decryption
            global_list_dfs = [self.df]
            del self.df
            result = self.decrypt_fields(0)
            del global_list_dfs

        return (result, decryption_failed) if self.pd_alert else result

    def decrypt_fields(self, index: int) -> pd.DataFrame:

        # Access Global Defined
        df = global_list_dfs[index]

        for d in self.output_mapping:
            # Initialize input variables
            input_field = d['input']
            output_field = d['output']
            salt_field = '%s_salt' % input_field
            iv_field = '%s_iv' % input_field
            encryption_config = f'{input_field}_encryption_config' if f'{input_field}_encryption_config' in df.columns else ''

            #             print(f'decrpting {input_field}')
            df[input_field] = df.apply(lambda row: self.decrypt(
                self.key_2 if self.key_2 and pd.notna(row.loc[encryption_config]) else self.key_1
                , row.loc[input_field]
                , row.loc[salt_field]
                , row.loc[iv_field]) if self.filters(input_field, row) else row.loc[input_field], axis=1)
            df.drop(columns=[salt_field, iv_field, encryption_config],
                    axis=1,
                    inplace=True)

            df.rename(columns={input_field: output_field}, inplace=True)

        return df

    def decrypt(self, key, value, salt, iv):
        if key is None or value is None or pd.isna(value) or salt is None or iv is None:
            return None
        else:
            value = b64decode(value)
            iv = b64decode(iv)
            salt = b64decode(salt) if len(salt) == 26 else bytes(salt, "utf-8")
            salted_key = self.__salt_key(bytes(key, "utf-8"), salt)
            cipher = AES.new(salted_key, AES.MODE_CBC, iv)
            pt = unpad(cipher.decrypt(value), AES.block_size)
            return pt.decode(self.encoding)

    @lru_cache(maxsize=None)
    # Decorator to wrap a function with a memoizing callable that saves up to the maxsize most recent calls.
    # It can save time when an expensive or I/O bound function is periodically called with the same arguments.
    # https://docs.python.org/3/library/functools.html
    def __salt_key(self, key, salt, key_length=32):
        return hashlib.pbkdf2_hmac('sha1', key, salt, 2000, key_length)

    def filters(self, field_name: str, row: pd.Series) -> bool:
        if self.filter_config:
            return self.__time_filters(
                field_name,
                row,
                **self.filter_config)
        else:
            return True

    def __time_filters(
            self,
            field_name: str,
            row: pd.Series,
            start_timestamp: str,
            end_timestamp: str,
            exclude_ids: List,
            *args,
            **kargs) -> bool:
        field_name = field_name.lower()
        existing_fields = [
            "encrypted_ssn",
            "encrypted_date_of_birth",
            "encrypted_letter_last_four_digits_ssn",
            "encrypted_bank_account_number"]
        return ((pd.to_datetime(start_timestamp) < pd.to_datetime(row.loc['event_created_time']) < pd.to_datetime(
            end_timestamp) and (row.loc['application_id']) not in exclude_ids) or field_name in existing_fields)