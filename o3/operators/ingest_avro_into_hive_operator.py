# -*- coding: utf-8 -*-
"""Custom operator for picking up a local Avro file and inserting it into Hive.
"""

import os

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from ..hooks.hdfs_hook import HDFSHook
from ..hooks.pyhive_hook import PyHiveHook


INPUT_FMT = 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUT_FMT = 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
ROW_FORMAT = 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'


class IngestAvroIntoHiveOperator(BaseOperator):
    """Moves Avro file into HDFS and inserts into a Hive table.

    :param str target_table: Hive table name to insert into.
    :param str avro_schema_path: Avro schema path in HDFS.
    :param src_filepath: Local Avro file path or callable that produces one.
    :param str hdfs_processing_dir: Directory to use for processing.
    :param bool remove_src: Remove Avro source file.
    """
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(self, target_table: str, avro_schema_path: str,
                 src_filepath, hdfs_processing_dir: str,
                 remove_src: bool = True, *args, **kwargs):
        super(IngestAvroIntoHiveOperator, self).__init__(*args, **kwargs)

        self.target_table = target_table
        self.avro_schema_path = avro_schema_path

        if isinstance(src_filepath, str):
            self.src_filepath_str = src_filepath
            self.src_filepath_callable = None
        elif callable(src_filepath):
            self.src_filepath_str = None
            self.src_filepath_callable = src_filepath
        else:
            raise AirflowException(
                f'Incompatible src_filepath {src_filepath!r}.')

        self.hdfs_processing_dir = hdfs_processing_dir.rstrip('/')
        self.remove_src = remove_src

    def execute(self, context) -> None:
        target_table = self.target_table
        avro_schema_path = self.avro_schema_path
        src_filepath = self.src_filepath_str or self.src_filepath_callable(
            **context)
        tmp_avro_path = os.path.join(self.hdfs_processing_dir,
                                     os.path.basename(src_filepath))

        hdfs = HDFSHook().get_conn()

        if not hdfs.exists(avro_schema_path):
            raise AirflowException(f'Avro schema {avro_schema_path} not found.')
        full_schema_path = f'hdfs://{hdfs.host}:{hdfs.port}{avro_schema_path}'

        if not hdfs.exists(tmp_avro_path):
            self.log.info(f'Moving local file {src_filepath} to '
                          f'HDFS {tmp_avro_path}')
            hdfs.put(src_filepath, tmp_avro_path, replication=1)

            if self.remove_src:
                self.log.info(f'Removing {src_filepath}.')
                os.remove(src_filepath)

        conn = PyHiveHook().get_conn()

        cursor = conn.cursor()

        temp_table_name = os.path.basename(src_filepath).replace(
            '.', '_').replace('-', '_')

        create_temp_table_stmt = f"""
            CREATE TABLE IF NOT EXISTS {temp_table_name}
            ROW FORMAT SERDE '{ROW_FORMAT}'
            STORED AS INPUTFORMAT '{INPUT_FMT}'
            OUTPUTFORMAT '{OUTPUT_FMT}'
            TBLPROPERTIES ('avro.schema.url'='{full_schema_path}')
        """
        print(f'--- create_temp_table_stmt ---\n{create_temp_table_stmt}')
        cursor.execute(create_temp_table_stmt)

        select_temp_row_stmt = f"""
            SELECT * FROM {temp_table_name} LIMIT 1
        """
        print(f'--- select_temp_row_stmt---\n{select_temp_row_stmt}')
        cursor.execute(select_temp_row_stmt)

        if cursor.fetchone() is None:
            load_data_stmt = f"""
                LOAD DATA INPATH '{tmp_avro_path}'
                INTO TABLE {temp_table_name}
            """
            print(f'--- load_data_stmt ---\n{load_data_stmt}')
            cursor.execute(load_data_stmt)

        create_target_table_stmt = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {target_table}
            PARTITIONED BY (ds STRING, h STRING, en STRING)
            ROW FORMAT SERDE '{ROW_FORMAT}'
            STORED AS INPUTFORMAT '{INPUT_FMT}'
            OUTPUTFORMAT '{OUTPUT_FMT}'
            TBLPROPERTIES ('avro.schema.url'='{full_schema_path}')
        """
        print(f'--- create_target_table_stmt ---\n{create_target_table_stmt}')
        cursor.execute(create_target_table_stmt)

        insert_data_stmt = f"""
            INSERT INTO {target_table} PARTITION (ds, h, en)
            SELECT
                *,
                datestamp AS ds,
                substr(server_date, 12, 2) AS h,
                event_name AS en
            FROM
                {temp_table_name}
        """
        print(f'--- insert_data_stmt ---\n{insert_data_stmt}')
        cursor.execute(insert_data_stmt)

        drop_temp_table_stmt = f"""
            DROP TABLE {temp_table_name}
        """
        print(f'--- drop_temp_table_stmt ---\n{drop_temp_table_stmt}')
        cursor.execute(drop_temp_table_stmt)
