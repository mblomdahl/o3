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

from airflow import DAG
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

with DAG('dag1', default_args=default_args, schedule_interval=timedelta(seconds=30), catchup=False) as dag1:

    s0 = FileSensor(task_id='scan_input_dir', fs_conn_id='fs_default',
                    filepath=path.join(getcwd(), 'input/'))

    def _get_input_file(*args, **kwargs):
        print('_get_input_file')
        print(pformat(args), pformat(kwargs))
        if not path.isdir('./process'):
            makedirs('./process')

        filenames = list(glob('input/*.txt'))
        if len(filenames):
            source_path = filenames[0]
            target_path = f'./process/{ path.basename(filenames[0]) }'
            print(f'Found input file { source_path }, moving to { target_path }.')
            move(source_path, target_path)
            return target_path
        else:
            raise AirflowSkipException('No files found.')

    t0 = PythonOperator(task_id='get_input_file', python_callable=_get_input_file, provide_context=True,
                        depends_on_past=True)

    def _count_words(*args, **kwargs):
        print('_count_words')
        print(pformat(args), pformat(kwargs))
        process_filepath = kwargs['task_instance'].xcom_pull(task_ids='get_input_file')
        sleep(5)
        with open(process_filepath, 'r', encoding='utf-8') as file_obj:
            spaces = file_obj.read().replace('\n', ' ').count(' ')
            return f'counted { spaces + 1 } words'

    t1a = PythonOperator(task_id='count_words', python_callable=_count_words, provide_context=True)

    def _count_rows(*args, **kwargs):
        print('_count_rows')
        print(pformat(args), pformat(kwargs))
        process_filepath = kwargs['task_instance'].xcom_pull(task_ids='get_input_file')
        sleep(10)
        with open(process_filepath, 'r', encoding='utf-8') as file_obj:
            rows = len(file_obj.read().splitlines())
            return f'found { rows } rows'

    t1b = PythonOperator(task_id='count_rows', python_callable=_count_rows, provide_context=True)

    def _summarize(*args, **kwargs):
        print('_summarize')
        print(pformat(args), pformat(kwargs))
        words, rows = kwargs['task_instance'].xcom_pull(task_ids=['count_words', 'count_rows'])
        return 'summary: ' + words + ' / ' + rows

    t2 = PythonOperator(task_id='summarize', python_callable=_summarize, provide_context=True)

    def _remove_input(*args, **kwargs):
        print('_remove_input')
        print(pformat(args), pformat(kwargs))
        process_filepath = kwargs['task_instance'].xcom_pull(task_ids='get_input_file')
        remove(process_filepath)

    t3 = PythonOperator(task_id='remove_input', python_callable=_remove_input, provide_context=True)

    # Workflow!
    s0 >> t0 >> [t1a, t1b] >> t2 >> t3


dag1.doc_md = __doc__
