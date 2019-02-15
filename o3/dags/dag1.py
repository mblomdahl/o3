"""
### A stab at https://github.com/mblomdahl/o3/issues/10

Every hour:

* Waits for something to show up in `$AIRFLOW_HOME/input/` dir
* Moves *.txt files in input dir to `$AIRFLOW_HOME/processing/`
* Primitive word and row counts in 2 parallel operations
* Prints a summary of word/row counts
* Deletes the processed input file

Totally awesome, right!

"""

from os import path
from datetime import timedelta

from airflow import DAG, conf
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator

from o3.operators.ensure_dir_operator import EnsureDirOperator
from o3.operators.move_file_operator import MoveFileOperator
from o3.operators.word_count_operator import WordCountOperator
from o3.operators.row_count_operator import RowCountOperator
from o3.operators.remove_file_operator import RemoveFileOperator

from o3.constants import DEFAULT_ARGS


FILE_INPUT_DIR = path.join(conf.AIRFLOW_HOME, 'input')

PROCESSING_DIR = path.join(conf.AIRFLOW_HOME, 'processing')


def _get_input_filepath(**ctx):
    return ctx['ti'].xcom_pull(task_ids='o3_t_get_input_file')


def _summarize_counts(**ctx):
    words, rows = ctx['task_instance'].xcom_pull(
        task_ids=['o3_t_count_words', 'o3_t_count_rows'])
    return f'summary: {words} / {rows}'


with DAG('o3_d_dag1', default_args=DEFAULT_ARGS,
         schedule_interval=timedelta(minutes=60), catchup=False) as _dag:

    ensure_dirs = EnsureDirOperator(task_id='o3_t_ensure_dirs_exist',
                                    paths=[FILE_INPUT_DIR, PROCESSING_DIR],
                                    fs_type='local')

    scan_input_dir = FileSensor(task_id='o3_s_scan_input_dir',
                                fs_conn_id='fs_default',
                                filepath=FILE_INPUT_DIR)

    move_input_file = MoveFileOperator(task_id='o3_t_get_input_file',
                                       glob_pattern='*.txt',
                                       src_dir=FILE_INPUT_DIR,
                                       dest_dir=PROCESSING_DIR,
                                       src_fs_type='local',
                                       dest_fs_type='local',
                                       max_files=1,
                                       depends_on_past=True)

    count_words = WordCountOperator(task_id='o3_t_count_words',
                                    filepath=_get_input_filepath,
                                    fs_type='local')

    count_rows = RowCountOperator(task_id='o3_t_count_rows',
                                  filepath=_get_input_filepath,
                                  fs_type='local')

    summarize_counts = PythonOperator(task_id='o3_t_summarize',
                                      python_callable=_summarize_counts,
                                      provide_context=True)

    remove_input_file = RemoveFileOperator(task_id='o3_t_remove_input',
                                           filepath=_get_input_filepath,
                                           fs_type='local')

    # Workflow!
    ensure_dirs >> scan_input_dir >> move_input_file >> \
        [count_words, count_rows] >> summarize_counts >> remove_input_file


_dag.doc_md = __doc__
