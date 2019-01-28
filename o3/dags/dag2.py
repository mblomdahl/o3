# -*- coding: utf-8 -*-
"""
### A stab at DAG 2 (https://github.com/mblomdahl/o3/issues/13)

Preparation:

* Go to web UI, _Admin_ > _Connections_ > _Create_
* Configure new HDFS type connection with ID `hdfs_default`
* Save it and trigger `o3_d_dag2` manually

What it does:

* Ensures the necessary directories exists in HDFS
* Waits for something to show up in HDFS `/user/airflow/input/` dir
* Moves *.txt files in input dir to HDFS `/user/airflow/processing/`
* Primitive word and row counts in 2 parallel operations
* Prints a summary of word/row counts
* Deletes the processed input file


"""

from os import path
from time import sleep
from pprint import pformat
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python_operator import PythonOperator

from o3.hooks.hdfs_hook import HDFSHook
from o3.sensors.hdfs_sensor import HDFSSensor


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

FILE_INPUT_DIR = path.join('/user/airflow/', 'input')

PROCESSING_DIR = path.join('/user/airflow/', 'processing')


with DAG('o3_d_dag2', default_args=default_args, schedule_interval='@once',
         catchup=False) as dag2:

    def _o3_t_ensure_dirs_exist():
        hdfs = HDFSHook().get_conn()
        if hdfs.isdir(FILE_INPUT_DIR):
            print(f'Input HDFS dir {FILE_INPUT_DIR} exists.')
        else:
            print(f'Creating HDFS {FILE_INPUT_DIR} directory.')
            hdfs.mkdir(FILE_INPUT_DIR)

        if hdfs.isdir(PROCESSING_DIR):
            print(f'Processing HDFS dir {PROCESSING_DIR} exists.')
        else:
            print(f'Creating HDFS {PROCESSING_DIR} directory.')
            hdfs.mkdir(PROCESSING_DIR)

    t0 = PythonOperator(task_id='o3_t_ensure_dirs_exist',
                        python_callable=_o3_t_ensure_dirs_exist)

    s0 = HDFSSensor(task_id='o3_s_scan_input_dir', filepath=FILE_INPUT_DIR)

    def _o3_t_get_input_file(*args, **kwargs):
        print('_o3_t_get_input_file', pformat(args), pformat(kwargs))
        hdfs = HDFSHook().get_conn()
        filenames = hdfs.glob(f'{FILE_INPUT_DIR}/*.txt')
        if len(filenames):
            source_path = filenames[0]
            target_path = f'{PROCESSING_DIR}/{ path.basename(filenames[0]) }'
            print(f'Found file { source_path }, moving to { target_path }.')
            hdfs.mv(source_path, target_path)
            return target_path
        else:
            raise AirflowSkipException('No files found.')

    t1 = PythonOperator(task_id='o3_t_get_input_file',
                        python_callable=_o3_t_get_input_file,
                        depends_on_past=True)

    def _o3_t_count_words(*args, **kwargs):
        print('_o3_t_count_words', pformat(args), pformat(kwargs))
        hdfs = HDFSHook().get_conn()
        process_filepath = kwargs['task_instance'].xcom_pull(
            task_ids='o3_t_get_input_file')

        sleep(5)
        with hdfs.open(process_filepath, 'rb') as file_obj:
            spaces = file_obj.read().decode('utf-8').replace(
                '\n', ' ').count(' ')
            return f'counted {spaces + 1} words'

    t2a = PythonOperator(task_id='o3_t_count_words',
                         python_callable=_o3_t_count_words,
                         provide_context=True)

    def _o3_t_count_rows(*args, **kwargs):
        print('__o3_t_count_rows', pformat(args), pformat(kwargs))
        hdfs = HDFSHook().get_conn()
        process_filepath = kwargs['task_instance'].xcom_pull(
            task_ids='o3_t_get_input_file')

        sleep(10)
        with hdfs.open(process_filepath, 'rb') as file_obj:
            rows = len(file_obj.read().decode('utf-8').splitlines())
            return f'found {rows} rows'

    t2b = PythonOperator(task_id='o3_t_count_rows',
                         python_callable=_o3_t_count_rows,
                         provide_context=True)

    def _o3_t_summarize(*args, **kwargs):
        print('_o3_t_summarize', pformat(args), pformat(kwargs))
        words, rows = kwargs['task_instance'].xcom_pull(
            task_ids=['o3_t_count_words', 'o3_t_count_rows'])

        return f'summary: {words} / {rows}'

    t3 = PythonOperator(task_id='o3_t_summarize',
                        python_callable=_o3_t_summarize,
                        provide_context=True)

    def _o3_t_remove_input(*args, **kwargs):
        print('_o3_t_remove_input', pformat(args), pformat(kwargs))
        hdfs = HDFSHook().get_conn()
        process_filepath = kwargs['task_instance'].xcom_pull(
            task_ids='o3_t_get_input_file')

        hdfs.rm(process_filepath)

    t4 = PythonOperator(task_id='o3_t_remove_input',
                        python_callable=_o3_t_remove_input,
                        provide_context=True)

    # Workflow!
    t0 >> s0 >> t1 >> [t2a, t2b] >> t3 >> t4


dag2.doc_md = __doc__
