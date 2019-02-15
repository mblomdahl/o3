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

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from o3.sensors.hdfs_sensor import HDFSSensor
from o3.operators.ensure_dir_operator import EnsureDirOperator
from o3.operators.move_file_operator import MoveFileOperator
from o3.operators.word_count_operator import WordCountOperator
from o3.operators.row_count_operator import RowCountOperator
from o3.operators.remove_file_operator import RemoveFileOperator

from o3.constants import DEFAULT_ARGS


FILE_INPUT_DIR = path.join('/user/airflow/', 'input')

PROCESSING_DIR = path.join('/user/airflow/', 'processing')


def _get_input_filepath(**ctx):
    return ctx['ti'].xcom_pull(task_ids='o3_t_get_input_file')


def _summarize_counts(**ctx):
    words, rows = ctx['task_instance'].xcom_pull(
        task_ids=['o3_t_count_words', 'o3_t_count_rows'])
    return f'summary: {words} / {rows}'


with DAG('o3_d_dag2', default_args=DEFAULT_ARGS, schedule_interval='@once',
         catchup=False) as _dag:

    ensure_dirs = EnsureDirOperator(task_id='o3_t_ensure_dirs_exist',
                                    paths=[FILE_INPUT_DIR, PROCESSING_DIR],
                                    fs_type='hdfs')

    scan_input_dir = HDFSSensor(task_id='o3_s_scan_input_dir',
                                hdfs_conn_id='hdfs_default',
                                filepath=FILE_INPUT_DIR)

    move_input_file = MoveFileOperator(task_id='o3_t_get_input_file',
                                       glob_pattern='*.txt',
                                       src_dir=FILE_INPUT_DIR,
                                       dest_dir=PROCESSING_DIR,
                                       src_fs_type='hdfs',
                                       dest_fs_type='hdfs',
                                       max_files=1,
                                       depends_on_past=True)

    count_words = WordCountOperator(task_id='o3_t_count_words',
                                    filepath=_get_input_filepath,
                                    fs_type='hdfs')

    count_rows = RowCountOperator(task_id='o3_t_count_rows',
                                  filepath=_get_input_filepath,
                                  fs_type='hdfs')

    summarize_counts = PythonOperator(task_id='o3_t_summarize',
                                      python_callable=_summarize_counts,
                                      provide_context=True)

    remove_input_file = RemoveFileOperator(task_id='o3_t_remove_input',
                                           filepath=_get_input_filepath,
                                           fs_type='hdfs')

    # Workflow!
    ensure_dirs >> scan_input_dir >> move_input_file >> \
        [count_words, count_rows] >> summarize_counts >> remove_input_file


_dag.doc_md = __doc__
