"""
## A stab at https://github.com/mblomdahl/o3/issues/10

Totally awesome, right!

"""

from os import path, remove, makedirs, getcwd
from glob import glob
from time import sleep
from pprint import pformat
from datetime import datetime, timedelta
from shutil import move

from airflow import DAG, conf
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.exceptions import AirflowSkipException
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'mblomdahl',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 18),
    'email': ['mats.blomdahl@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

FILE_INPUT_DIR = path.join(conf.AIRFLOW_HOME, 'input')

PROCESSING_DIR = path.join(conf.AIRFLOW_HOME, 'processing')


with DAG('dag1', default_args=default_args, schedule_interval=timedelta(seconds=30), catchup=False) as dag1:

    def _ensure_dirs_exist():
        if not path.isdir(FILE_INPUT_DIR):
            print(f'Creating {FILE_INPUT_DIR} directory.')
            makedirs(FILE_INPUT_DIR)
        else:
            print(f'Input dir {FILE_INPUT_DIR} exists.')

        if not path.isdir(PROCESSING_DIR):
            print(f'Creating {PROCESSING_DIR} directory.')
            makedirs(PROCESSING_DIR)
        else:
            print(f'Processing dir {PROCESSING_DIR} exists.')

    t0 = PythonOperator(task_id='ensure_dirs_exist', python_callable=_ensure_dirs_exist)

    s0 = FileSensor(task_id='scan_input_dir', fs_conn_id='fs_default',
                    filepath=path.join(conf.AIRFLOW_HOME, 'input/'))

    def _get_input_file(*args, **kwargs):
        print('_get_input_file', pformat(args), pformat(kwargs))

        filenames = list(glob(f'{FILE_INPUT_DIR}/*.txt'))
        if len(filenames):
            source_path = filenames[0]
            target_path = f'{PROCESSING_DIR}/{ path.basename(filenames[0]) }'
            print(f'Found input file { source_path }, moving to { target_path }.')
            move(source_path, target_path)
            return target_path
        else:
            raise AirflowSkipException('No files found.')

    t1 = PythonOperator(task_id='get_input_file', python_callable=_get_input_file, provide_context=True,
                        depends_on_past=True)

    def _count_words(*args, **kwargs):
        print('_count_words', pformat(args), pformat(kwargs))
        process_filepath = kwargs['task_instance'].xcom_pull(task_ids='get_input_file')
        sleep(5)
        with open(process_filepath, 'r', encoding='utf-8') as file_obj:
            spaces = file_obj.read().replace('\n', ' ').count(' ')
            return f'counted { spaces + 1 } words'

    t2a = PythonOperator(task_id='count_words', python_callable=_count_words, provide_context=True)

    def _count_rows(*args, **kwargs):
        print('_count_rows', pformat(args), pformat(kwargs))
        process_filepath = kwargs['task_instance'].xcom_pull(task_ids='get_input_file')
        sleep(10)
        with open(process_filepath, 'r', encoding='utf-8') as file_obj:
            rows = len(file_obj.read().splitlines())
            return f'found { rows } rows'

    t2b = PythonOperator(task_id='count_rows', python_callable=_count_rows, provide_context=True)

    def _summarize(*args, **kwargs):
        print('_summarize', pformat(args), pformat(kwargs))
        words, rows = kwargs['task_instance'].xcom_pull(task_ids=['count_words', 'count_rows'])
        return 'summary: ' + words + ' / ' + rows

    t3 = PythonOperator(task_id='summarize', python_callable=_summarize, provide_context=True)

    def _remove_input(*args, **kwargs):
        print('_remove_input', pformat(args), pformat(kwargs))
        process_filepath = kwargs['task_instance'].xcom_pull(task_ids='get_input_file')
        remove(process_filepath)

    t4 = PythonOperator(task_id='remove_input', python_callable=_remove_input, provide_context=True)

    # Workflow!
    t0 >> s0 >> t1 >> [t2a, t2b] >> t3 >> t4


dag1.doc_md = __doc__
