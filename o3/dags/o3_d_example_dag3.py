# -*- coding: utf-8 -*-
"""
### Example DAG 3 in Action (https://github.com/mblomdahl/o3/issues/21)

Preparation:

* Go to web UI, _Admin_ > _Connections_ > _Create_
* Configure HDFS type connection with ID `hdfs_default`
* Configure HiveServer2 connection with ID `hiveserver2_default`
* Place Avro schema in HDFS as `/user/airflow/o3_analytics.avcs` and
  locally as `/tmp/o3_analytics.avcs`

What it does:

1. Ensures the necessary directories exists in HDFS
2. Waits for something to show up in local `/home/airflow/airflow_home/input/`
3. Filters `event.log*` files into `/home/airflow/airflow_home/processing/` dir
4. Splits filtered log files into a bunch of smaller log files
5. Converts each of the smaller log files to Avro format
6. Inserts the Avro files in a Hive table

"""

from os import path

from airflow import DAG, conf

from airflow.contrib.sensors.file_sensor import FileSensor

from o3.operators.ensure_dir_operator import EnsureDirOperator
from o3.operators.filter_logs_to_percentage_operator import \
    FilterLogsToPercentageOperator
from o3.operators.split_log_by_classifiers_operator import \
    SplitLogByClassifiersOperator
from o3.operators.convert_log_to_avro_operator import \
    ConvertLogToAvroOperator
from o3.operators.ingest_avro_into_hive_operator import \
    IngestAvroIntoHiveOperator

from o3.constants import DEFAULT_ARGS, LOG_CLASSIFIERS

LOCAL_INPUT_DIR = path.join(conf.AIRFLOW_HOME, 'input')

LOCAL_PROCESSING_DIR = path.join(conf.AIRFLOW_HOME, 'processing')

HDFS_PROCESSING_DIR = path.join('/user/airflow/', 'processing')


def _get_split_input_filepath_for_classifier(classifier_name: str):
    def _get_split_input_filepath(**ctx):
        print(f'Looking for classifier {classifier_name} in {ctx!r}...')
        return ctx['ti'].xcom_pull(task_ids='o3_t_split_by_classifier')[
            'output_path_by_classifier'].get(classifier_name)
    return _get_split_input_filepath


def _get_avro_output_path_for(task_id: str):
    def _get_avro_output_path(**ctx):
        return ctx['ti'].xcom_pull(task_ids=task_id)['output_path']
    return _get_avro_output_path


with DAG('o3_d_example_dag3', default_args=DEFAULT_ARGS, schedule_interval='@once',
         catchup=False) as _dag:
    ensure_local_and_hdfs_dirs = [
        EnsureDirOperator(
            task_id='o3_t_ensure_local_dirs_exist',
            paths=[LOCAL_INPUT_DIR, LOCAL_PROCESSING_DIR],
            fs_type='local'
        ),
        EnsureDirOperator(
            task_id='o3_t_ensure_hdfs_dirs_exist',
            paths=[HDFS_PROCESSING_DIR],
            fs_type='hdfs'
        )
    ]

    scan_input_dir = FileSensor(
        task_id='o3_s_scan_input_dir',
        fs_conn_id='fs_default',
        filepath=LOCAL_INPUT_DIR
    )

    filter_out_3pct = FilterLogsToPercentageOperator(
        task_id='o3_t_filter_to_3_pct',
        percentage=3.0,
        glob_pattern='events.log*',
        src_dir=LOCAL_INPUT_DIR,
        dest_dir=LOCAL_PROCESSING_DIR,
        max_files=1,
        remove_src=True,
        depends_on_past=True
    )

    split_logs = SplitLogByClassifiersOperator(
        task_id='o3_t_split_by_classifier',
        classifiers=LOG_CLASSIFIERS,
        src_filepath=lambda **ctx: ctx['ti'].xcom_pull(
            task_ids='o3_t_filter_to_3_pct')[0],
        remove_src=True
    )

    convert_to_avro_and_ingest_into_hive = []

    for classifier in LOG_CLASSIFIERS:
        convert_to_avro = ConvertLogToAvroOperator(
            task_id=f'o3_t_convert_{classifier}_to_avro',
            avro_schema_path='/tmp/o3_analytics.avsc',
            src_filepath=_get_split_input_filepath_for_classifier(classifier),
            validate_percentage=10.0,
            remove_src=True,
            depends_on_past=True
        )

        ingest_into_hive = IngestAvroIntoHiveOperator(
            task_id=f'o3_t_ingest_{classifier}_into_hive',
            target_table='logevents_ds',
            avro_schema_path='/user/airflow/o3_analytics.avsc',
            src_filepath=_get_avro_output_path_for(convert_to_avro.task_id),
            hdfs_processing_dir=HDFS_PROCESSING_DIR,
            remove_src=True
        ).set_upstream(convert_to_avro)

        convert_to_avro_and_ingest_into_hive.append(convert_to_avro)

    # Workflow!
    ensure_local_and_hdfs_dirs >> scan_input_dir >> filter_out_3pct \
        >> split_logs >> convert_to_avro_and_ingest_into_hive

_dag.doc_md = __doc__
