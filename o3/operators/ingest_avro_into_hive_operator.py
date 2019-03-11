# -*- coding: utf-8 -*-
"""Custom operator for picking up a local Avro file and inserting it into Hive.
"""

import os

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from ..hooks.hdfs_hook import HDFSHook
from ..hooks.pyhive_hook import PyHiveHook

from ..utils import create_concat_filename, concat_avro_files


INPUT_FMT = 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUT_FMT = 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
ROW_FORMAT = 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'


class IngestAvroIntoHiveOperator(BaseOperator):
    """Moves Avro file into HDFS and inserts into a Hive table.

    :param str target_table: Hive table name to insert into.
    :param str avro_schema_path: Avro schema path in HDFS.
    :param src_filepath: Local Avro file path or callable that produces one,
                         supports a single path or list of paths (templated).
    :param str hdfs_processing_dir: Directory to use for processing.
    :param bool concat_src: Concatenate Avro source files before loading.
    :param bool remove_src: Remove Avro source file.
    """
    template_fields = ['src_filepath_str']
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(self, target_table: str, avro_schema_path: str,
                 src_filepath, hdfs_processing_dir: str,
                 concat_src: bool = True, remove_src: bool = True,
                 *args, **kwargs):
        super(IngestAvroIntoHiveOperator, self).__init__(*args, **kwargs)

        self.target_table = target_table
        self.avro_schema_path = avro_schema_path

        if isinstance(src_filepath, (str, list)):
            self.src_filepath_str = src_filepath
            self.src_filepath_callable = None
        elif callable(src_filepath):
            self.src_filepath_str = None
            self.src_filepath_callable = src_filepath
        else:
            raise AirflowException(
                f'Incompatible src_filepath {src_filepath!r}.')

        self.hdfs_processing_dir = hdfs_processing_dir.rstrip('/')
        self.concat_src = concat_src
        self.remove_src = remove_src

    def _concat_src_files(self, src_filepaths: list) -> list:
        src_concat_path = create_concat_filename(*src_filepaths)

        for src_path in src_filepaths:
            if not os.path.exists(src_path):
                if os.path.exists(src_concat_path):
                    # Give up and rely on concatenated source from last run.
                    return [src_concat_path]
                else:
                    raise AssertionError(f"Don't know what to do with "
                                         f"non-existent source file {src_path}")

        concat_avro_files(src_filepaths, src_concat_path)
        if self.remove_src:
            for src_path in src_filepaths:
                self.log.info(f'Removing {src_path}.')
                os.remove(src_path)

        return [src_concat_path]

    def _move_src_files_to_hdfs(self, src_filepaths: list) -> list:
        temp_avro_paths = []
        hdfs = HDFSHook().get_conn()

        for src_filepath in src_filepaths:
            temp_avro_path = os.path.join(self.hdfs_processing_dir,
                                          os.path.basename(src_filepath))
            if not hdfs.exists(temp_avro_path):
                self.log.info(f'Moving local file {src_filepath} to '
                              f'HDFS {temp_avro_path}')
                try:
                    hdfs.put(src_filepath, temp_avro_path, replication=1)
                except FileNotFoundError as err:
                    self.log.error(f'Upload failed: {err}')
            temp_avro_paths.append(temp_avro_path)

        return temp_avro_paths

    @staticmethod
    def _load_avro_into_temp_tables(temp_avro_paths: list,
                                    full_schema_path: str) -> list:
        temp_table_names = []
        conn = PyHiveHook().get_conn()
        cursor = conn.cursor()

        for temp_avro_path in temp_avro_paths:
            temp_table_name = os.path.basename(temp_avro_path).replace(
                '.', '_').replace('-', '_')

            create_temp_table_stmt = f"""
                CREATE TABLE IF NOT EXISTS {temp_table_name}
                ROW FORMAT SERDE '{ROW_FORMAT}'
                STORED AS INPUTFORMAT '{INPUT_FMT}'
                OUTPUTFORMAT '{OUTPUT_FMT}'
                TBLPROPERTIES ('avro.schema.url'='{full_schema_path}')
            """
            print('--- create_temp_table_stmt ---')
            cursor.execute(create_temp_table_stmt)

            select_temp_row_stmt = f"""
                SELECT * FROM {temp_table_name} LIMIT 1
            """
            print('--- select_temp_row_stmt ---')
            cursor.execute(select_temp_row_stmt)

            if cursor.fetchone() is None:
                load_data_stmt = f"""
                    LOAD DATA INPATH '{temp_avro_path}'
                    INTO TABLE {temp_table_name}
                """
                print('--- load_data_stmt ---')
                cursor.execute(load_data_stmt)

            temp_table_names.append(temp_table_name)

        return temp_table_names

    def _insert_temp_data_into_target_table(self, temp_table_names: list,
                                            full_schema_path: str) -> None:
        conn = PyHiveHook().get_conn()
        cursor = conn.cursor()

        create_target_table_stmt = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {self.target_table}
            PARTITIONED BY (ds STRING, h STRING, en STRING)
            ROW FORMAT SERDE '{ROW_FORMAT}'
            STORED AS INPUTFORMAT '{INPUT_FMT}'
            OUTPUTFORMAT '{OUTPUT_FMT}'
            TBLPROPERTIES ('avro.schema.url'='{full_schema_path}')
        """
        print('--- create_target_table_stmt ---')
        cursor.execute(create_target_table_stmt)

        for temp_table_name in temp_table_names:
            insert_data_stmt = f"""
                INSERT INTO {self.target_table} PARTITION (ds, h, en)
                SELECT
                    *,
                    datestamp AS ds,
                    substr(server_date, 12, 2) AS h,
                    event_name AS en
                FROM
                    {temp_table_name}
            """

            print('--- insert_data_stmt ---')
            cursor.execute(insert_data_stmt)

            drop_temp_table_stmt = f"""
                DROP TABLE {temp_table_name}
            """
            print('--- drop_temp_table_stmt ---')
            cursor.execute(drop_temp_table_stmt)

    def execute(self, context) -> None:
        avro_schema_path = self.avro_schema_path
        if self.src_filepath_callable:
            src_filepath = self.src_filepath_callable(**context)
        else:
            try:
                src_filepath = eval(self.src_filepath_str)
            except SyntaxError:
                src_filepath = self.src_filepath_str

        if not src_filepath:
            self.log.info('No filepath(s) received, skipping.')
            raise AirflowSkipException()

        if isinstance(src_filepath, list):
            src_filepaths = src_filepath
        else:
            src_filepaths = [src_filepath]

        if self.concat_src and len(src_filepaths) > 1:
            src_filepaths = self._concat_src_files(src_filepaths)

        hdfs = HDFSHook().get_conn()
        if not hdfs.exists(avro_schema_path):
            raise AirflowException(f'Avro schema {avro_schema_path} not found.')
        full_schema_path = f'hdfs://{hdfs.host}:{hdfs.port}{avro_schema_path}'

        temp_avro_paths = self._move_src_files_to_hdfs(src_filepaths)

        temp_table_names = self._load_avro_into_temp_tables(temp_avro_paths,
                                                            full_schema_path)

        self._insert_temp_data_into_target_table(temp_table_names,
                                                 full_schema_path)

        if self.remove_src:
            for src_filepath in src_filepaths:
                self.log.info(f'Removing {src_filepath}.')
                os.remove(src_filepath)
