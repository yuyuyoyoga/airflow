from datetime import date, datetime, timedelta
from pytz import timezone

run_date = date.today().strftime('%Y_%m_%d')
start_date = datetime.now(timezone('US/Central')).strftime('%Y%m%d')
end_date = (
    datetime.now(
        timezone('US/Central')) -
    timedelta(
        days=7)).strftime('%Y%m%d')
configs = [
    {
        'job_name': 'avant_psae_file',

         'job_configs': {
             'partner': 'avant',
             'courier_logging_table': 'avant_psae',
             'healthcheck_url': 'https://hcio.shared.prd.internal.amount.com/ping/59039184-2c2e-4303-b37a-2794c7f56d4a',
             'author_url': 'https://garden.internal.amount.com/tasks/pba-td-courier-applicant-files',
             'query_config': [
                 {
                     'source': 'presto',
                     'query_path': 'csv/psae.csv',
                     'primary_key': 'customer_id',
                     'query_params': {'measurement_date': run_date},
                     'qubole_env': 'prod',
                     'merge': None
                 },
                 {
                     'source': 'prod',
                     'query_path': 'csv/psae_bank.csv',
                     'qubole_env': 'prod',
                     'merge': {'type': 'join',
                               'key': 'hashed_account_number',
                               'direction': 'left'}
                 },
                 {
                     'source': 'prod',
                     'query_path': 'csv/psae_ssn.csv',
                     'qubole_env': 'prod',
                     'merge': {'type': 'join',
                               'key': 'customer_id',
                               'direction': 'left'}
                 }
             ],
             'final_columns': None,
             'decryption_config': {
                 'output_mapping': [{'input': 'encrypted_personal_identification', 'output': 'full_ssn'},
                                    {'input': 'encrypted_acct_number',
                                     'output': 'bank_account_number'}],
                 'filter_config': {}
             },
         },
         'file_configs': {

             'file_config': [
                 {
                     'file_name': f'avant_sar_{start_date}_{end_date}.json.gpg',
                     'field_customization': None,
                     'file_customization': None
                 },
                 {
                     'file_name': f'avant_sar_alpha_{start_date}_{end_date}.json.gpg',
                     'field_customization': None,
                     'file_customization': None
                 },
                 {
                     'file_name': f'avant_sar_Beta_{start_date}_{end_date}.json.gpg',
                     'field_customization': None,
                     'file_customization': None
                 }
             ],
             'pii_fields': ['letter_first_name', 'letter_last_name', 'letter_ssn', 'letter_address_1', 'letter_address_2',
                            'letter_city',
                            'letter_state', 'letter_zip', 'ssn_last_4', 'city', 'state', 'zip_code', 'phone',
                            'bank_account_number_last_4'],
         }
     },
    {
        'job_name': 'avant_app_file',

        'job_configs': {
            'partner': 'avant',
            'courier_logging_table': 'avant_psae',
            'healthcheck_url': 'https://hcio.shared.prd.internal.amount.com/ping/59039184-2c2e-4303-b37a-2794c7f56d4a',
            'author_url': 'https://garden.internal.amount.com/tasks/pba-td-courier-applicant-files',
            'query_config': [
                {
                    'source': 'presto',
                    'query_path': 'csv/psae.csv',
                    'primary_key': 'customer_id',
                    'query_params': {'measurement_date': run_date},
                    'qubole_env': 'prod',
                    'merge': None
                },
                {
                    'source': 'prod',
                    'query_path': 'csv/psae_bank.csv',
                    'qubole_env': 'prod',
                    'merge': {'type': 'join',
                              'key': 'hashed_account_number',
                              'direction': 'left'}
                },
                {
                    'source': 'prod',
                    'query_path': 'csv/psae_ssn.csv',
                    'qubole_env': 'prod',
                    'merge': {'type': 'join',
                              'key': 'customer_id',
                              'direction': 'left'}
                }
            ],
            'final_columns': None,
            'decryption_config': {
                'output_mapping': [{'input': 'encrypted_personal_identification', 'output': 'full_ssn'},
                                   {'input': 'encrypted_acct_number',
                                    'output': 'bank_account_number'}],
                'filter_config': {}
            },
        },
        'file_configs': {

            'file_config': [
                {
                    'file_name': f'avant_applicant_{run_date}.txt.gpg',
                    'field_customization': None,
                    'file_customization': None
                },
                {
                    'file_name': f'avant_applicant_alpha_{run_date}.txt.gpg',
                    'field_customization': None,
                    'file_customization': None
                },
                {
                    'file_name': f'avant_applicant_beta_{run_date}.txt.gpg',
                    'field_customization': None,
                    'file_customization': None
                }
            ],
            'pii_fields': ['letter_first_name', 'letter_last_name', 'letter_ssn', 'letter_address_1',
                           'letter_address_2',
                           'letter_city',
                           'letter_state', 'letter_zip', 'ssn_last_4', 'city', 'state', 'zip_code', 'phone',
                           'bank_account_number_last_4'],
        }
    },
    {
        'job_name': 'avant_credit_file',

        'job_configs': {
            'partner': 'avant',
            'courier_logging_table': 'avant_psae',
            'healthcheck_url': 'https://hcio.shared.prd.internal.amount.com/ping/59039184-2c2e-4303-b37a-2794c7f56d4a',
            'author_url': 'https://garden.internal.amount.com/tasks/pba-td-courier-applicant-files',
            'query_config': [
                {
                    'source': 'presto',
                    'query_path': 'csv/psae.csv',
                    'primary_key': 'customer_id',
                    'query_params': {'measurement_date': run_date},
                    'qubole_env': 'prod',
                    'merge': None
                },
                {
                    'source': 'prod',
                    'query_path': 'csv/psae_bank.csv',
                    'qubole_env': 'prod',
                    'merge': {'type': 'join',
                              'key': 'hashed_account_number',
                              'direction': 'left'}
                },
                {
                    'source': 'prod',
                    'query_path': 'csv/psae_ssn.csv',
                    'qubole_env': 'prod',
                    'merge': {'type': 'join',
                              'key': 'customer_id',
                              'direction': 'left'}
                }
            ],
            'final_columns': None,
            'decryption_config': {
                'output_mapping': [{'input': 'encrypted_personal_identification', 'output': 'full_ssn'},
                                   {'input': 'encrypted_acct_number',
                                    'output': 'bank_account_number'}],
                'filter_config': {}
            },
        },
        'file_configs': {

            'file_config': [
                {
                    'file_name': f'avant_credit_{run_date}.txt.gpg',
                    'field_customization': None,
                    'file_customization': None
                },
                {
                    'file_name': f'avant_credit_alpha_{run_date}.txt.gpg',
                    'field_customization': None,
                    'file_customization': None
                },
                {
                    'file_name': f'avant_credit_beta_{run_date}.txt.gpg',
                    'field_customization': None,
                    'file_customization': None
                }
            ],
            'pii_fields': ['letter_first_name', 'letter_last_name', 'letter_ssn', 'letter_address_1',
                           'letter_address_2',
                           'letter_city',
                           'letter_state', 'letter_zip', 'ssn_last_4', 'city', 'state', 'zip_code', 'phone',
                           'bank_account_number_last_4'],
        }
    },
    {
        'job_name': 'avant_ver_file',

        'job_configs': {
            'partner': 'avant',
            'courier_logging_table': 'avant_psae',
            'healthcheck_url': 'https://hcio.shared.prd.internal.amount.com/ping/59039184-2c2e-4303-b37a-2794c7f56d4a',
            'author_url': 'https://garden.internal.amount.com/tasks/pba-td-courier-applicant-files',
            'query_config': [
                {
                    'source': 'presto',
                    'query_path': 'csv/psae.csv',
                    'primary_key': 'customer_id',
                    'query_params': {'measurement_date': run_date},
                    'qubole_env': 'prod',
                    'merge': None
                },
                {
                    'source': 'prod',
                    'query_path': 'csv/psae_bank.csv',
                    'qubole_env': 'prod',
                    'merge': {'type': 'join',
                              'key': 'hashed_account_number',
                              'direction': 'left'}
                },
                {
                    'source': 'prod',
                    'query_path': 'csv/psae_ssn.csv',
                    'qubole_env': 'prod',
                    'merge': {'type': 'join',
                              'key': 'customer_id',
                              'direction': 'left'}
                }
            ],
            'final_columns': None,
            'decryption_config': {
                'output_mapping': [{'input': 'encrypted_personal_identification', 'output': 'full_ssn'},
                                   {'input': 'encrypted_acct_number',
                                    'output': 'bank_account_number'}],
                'filter_config': {}
            },
        },
        'file_configs': {

            'file_config': [
                {
                    'file_name': f'avant_ver_{run_date}.txt.gpg',
                    'field_customization': None,
                    'file_customization': None
                },
                {
                    'file_name': f'avant_ver_alpha_{run_date}.txt.gpg',
                    'field_customization': None,
                    'file_customization': None
                },
                {
                    'file_name': f'avant_ver_beta_{run_date}.txt.gpg',
                    'field_customization': None,
                    'file_customization': None
                }
            ],
            'pii_fields': ['letter_first_name', 'letter_last_name', 'letter_ssn', 'letter_address_1',
                           'letter_address_2',
                           'letter_city',
                           'letter_state', 'letter_zip', 'ssn_last_4', 'city', 'state', 'zip_code', 'phone',
                           'bank_account_number_last_4'],
        }
    }
]