#!/usr/bin/env python3

import os
import json
import datetime
import click

import hdfs3
from pyhive import hive
from fastavro.read import reader, is_avro

from o3.constants import LOG_CLASSIFIERS
from o3.utils import split_log_by_classifier, filter_to_percentage, \
    convert_to_avro, concat_avro_files, create_concat_filename


@click.group()
def aac():
    """Does nothing, goes nowhere. Use sub-commands."""
    pass


@aac.command('filter_logs')
@click.argument('input_path', type=click.Path(exists=True, dir_okay=False))
@click.argument('percentage', type=float)
@click.argument('output_path', type=click.Path(dir_okay=False))
@click.option('-F', '--filter_str', type=str, multiple=True,
              help='Exact string to pre-filter input by')
def filter_logs(input_path: str, percentage: float, output_path: str,
                filter_str: list):
    """Filter out percentage of IDs to output path."""

    click.echo(filter_to_percentage(input_path, percentage, output_path,
                                    prefilter_by=filter_str))


@aac.command('split_logs')
@click.argument('log_path', type=click.Path(exists=True, dir_okay=False))
def split_logs(log_path: str):
    """Split log by classifiers."""

    click.echo(split_log_by_classifier(log_path, LOG_CLASSIFIERS))


@aac.command('to_avro')
@click.argument('schema_path', type=click.Path(exists=True, dir_okay=False))
@click.argument('log_path', type=click.Path(exists=True, dir_okay=False))
@click.option('-o', '--output_path', type=click.Path(dir_okay=False),
              default=None, help='Avro file output path')
@click.option('-p', '--validate_percentage', type=float,
              help='Percentage to validate', default=100.0)
@click.option('-o', '--offset', type=int, help='Start line offset', default=0)
@click.option('-l', '--limit', type=int, help='Max lines to convert')
def to_avro(schema_path: str, log_path: str, output_path: str,
            validate_percentage: float, offset: int, limit: int):
    """Convert log input to Avro format."""

    click.echo(convert_to_avro(schema_path, log_path,
                               output_path=output_path,
                               delete_existing_avro_file=True,
                               validate_percentage=validate_percentage,
                               offset=offset,
                               max_lines=limit))


@aac.command('compare_avro')
@click.argument('avro_path_a', type=click.Path(exists=True, dir_okay=False))
@click.argument('avro_path_b', type=click.Path(exists=True, dir_okay=False))
@click.option('-o', '--offset', type=int, help='Start line offset', default=0)
@click.option('-l', '--limit', type=int, help='Max lines to compare')
def compare_avro(avro_path_a: str, avro_path_b: str, offset: int, limit: int):
    """Compare Avro file contents."""

    click.echo(f'{datetime.datetime.utcnow().isoformat()[:19]}Z '
               f'Comparing Avro file {avro_path_a!r} to {avro_path_b!r} '
               f'(PID {os.getpid()})...')

    for input_path in (avro_path_a, avro_path_b):
        if not is_avro(input_path):
            raise ValueError(f'{input_path!r} is not an Avro file.')

    avro_file_b = open(avro_path_b, 'rb')
    avro_reader_b = reader(avro_file_b)

    avro_file_a = open(avro_path_a, 'rb')
    avro_reader_a = reader(avro_file_a)

    mismatched_a = []
    mismatched_b = []

    reports_per_uuid_a = {}
    reports_per_uuid_b = {}

    iteration = 0
    while not limit or iteration < (offset + limit):
        iteration += 1
        if iteration <= offset:
            avro_reader_a.next()
            avro_reader_b.next()
            continue

        if iteration % 1000 == 0 and (len(mismatched_a) or len(mismatched_b)):
            click.echo(
                f'{datetime.datetime.utcnow().isoformat()[:19]}Z '
                f'Iteration {str(iteration).zfill(7)}; '
                f'mismatched records from avro_path_a={len(mismatched_a)} and '
                f'avro_path_b={len(mismatched_b)}, unique UUIDs in '
                f'path a={len(reports_per_uuid_a)} and '
                f'b={len(reports_per_uuid_b)} '
                f'(PID {os.getpid()})...')

        try:
            rec_a = avro_reader_a.next()
        except StopIteration:
            click.echo(f'{datetime.datetime.utcnow().isoformat()[:19]}Z '
                       f'Iteration {str(iteration).zfill(7)}; '
                       f'reached end of {avro_path_a} '                       
                       f'(PID {os.getpid()}).')
            break

        try:
            rec_b = avro_reader_b.next()
        except StopIteration:
            click.echo(f'{datetime.datetime.utcnow().isoformat()[:19]}Z '
                       f'Iteration {str(iteration).zfill(7)}; '
                       f'reached end of {avro_path_b} '                       
                       f'(PID {os.getpid()}).')
            break

        if rec_a == rec_b:
            continue
        else:
            uuid_a = rec_a['uuid']
            if uuid_a not in reports_per_uuid_a:
                reports_per_uuid_a[uuid_a] = 1
            else:
                reports_per_uuid_a[uuid_a] += 1

            uuid_b = rec_b['uuid']
            if uuid_b not in reports_per_uuid_b:
                reports_per_uuid_b[uuid_b] = 1
            else:
                reports_per_uuid_b[uuid_b] += 1

        if rec_a in mismatched_b:
            mismatched_b.remove(rec_a)
        else:
            mismatched_a.append(rec_a)

        if rec_b in mismatched_a:
            mismatched_a.remove(rec_b)
        else:
            mismatched_b.append(rec_b)

    click.echo(
        f'{datetime.datetime.utcnow().isoformat()[:19]}Z '
        f'Final state of comparison; '
        f'mismatched records from avro_path_a={len(mismatched_a)} and '
        f'avro_path_b={len(mismatched_b)}, unique UUIDs in '
        f'path a={len(reports_per_uuid_a)} and '
        f'b={len(reports_per_uuid_b)} '
        f'(PID {os.getpid()}).')

    if len(mismatched_a) or len(mismatched_b):
        click.echo(f'{datetime.datetime.utcnow().isoformat()[:19]}Z '
                   f'Dumping mismatched objects as aac_compare_avro_a.json and '
                   f'aac_compare_avro_b.json.')

        def _sort_fn(rec):
            return (rec['uuid'] + rec['event_name'] + rec['server_date']
                    + rec['client_local_date'])

        with open('aac_compare_avro_a.json', 'wb') as json_file:
            json_file.write(json.dumps({
                'mismatched_a': sorted(mismatched_a, key=_sort_fn)
            }).encode())

        with open('aac_compare_avro_b.json', 'wb') as json_file:
            json_file.write(json.dumps({
                'mismatched_b': sorted(mismatched_b, key=_sort_fn)
            }).encode())


@aac.command('concat_avro')
@click.argument('input_paths', nargs=-1, type=click.Path(exists=True,
                                                         dir_okay=False))
@click.option('-o', '--output_path', type=click.Path(dir_okay=False),
              help='Concatenated output path', default=None)
@click.option('-t', '--avro_tools_path', type=str,
              help='Path to avro-tools jar file')
def concat_avro(input_paths: list, output_path: str, avro_tools_path: str):
    """Concatenate Avro files."""

    if not output_path:
        output_path = create_concat_filename(*input_paths)
    click.echo(concat_avro_files(input_paths, output_path,
                                 avro_tools_path=avro_tools_path))


@aac.command('ingest_avro')
@click.argument('schema_path', type=click.Path(exists=True, dir_okay=False))
@click.argument('avro_path', type=click.Path(exists=True, dir_okay=False))
@click.option('-t', '--target_table', type=str, help='Target table',
              default='logevents_ds')
@click.option('-h', '--host', type=str, help='Hostname', default='o3-master')
@click.option('--thrift_port', type=int, help='Thrift port', default=10000)
@click.option('--hdfs_port', type=int, help='HDFS port', default=9000)
@click.option('-u', '--username', type=str, help='Username', default='airflow')
def ingest_avro(schema_path: str, avro_path: str, target_table: str, host: str,
                thrift_port: int, hdfs_port: int, username: str):
    """Ingest Avro data into Hive."""

    fs = hdfs3.HDFileSystem(host=host, port=hdfs_port, user=username)

    schema_basename = os.path.basename(schema_path)
    hdfs_schema_path = os.path.join(f'/user/{username}', schema_basename)
    full_hdfs_schema_path = f'hdfs://{host}:{hdfs_port}{hdfs_schema_path}'
    if fs.exists(hdfs_schema_path):
        fs.rm(hdfs_schema_path)
    fs.put(schema_path, hdfs_schema_path, replication=1)

    avro_basename = os.path.basename(avro_path)
    hdfs_avro_path = os.path.join(f'/user/{username}', avro_basename)

    if not fs.exists(hdfs_avro_path):
        fs.put(avro_path, hdfs_avro_path, replication=1)

    conn = hive.Connection(host=host, port=thrift_port, username=username,
                           configuration={
                               'hive.exec.dynamic.partition.mode': 'nonstrict'
                           })
    cursor = conn.cursor()

    input_fmt = 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
    output_fmt = 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
    row_format = 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
    temp_table_name = avro_basename.replace('.', '_').replace('-', '_')

    create_temp_table_stmt = f"""
        CREATE TABLE IF NOT EXISTS {temp_table_name}
        ROW FORMAT SERDE '{row_format}'
        STORED AS INPUTFORMAT '{input_fmt}'
        OUTPUTFORMAT '{output_fmt}'
        TBLPROPERTIES ('avro.schema.url'='{full_hdfs_schema_path}')
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
            LOAD DATA INPATH '{hdfs_avro_path}'
            INTO TABLE {temp_table_name}
        """
        print(f'--- load_data_stmt ---\n{load_data_stmt}')
        cursor.execute(load_data_stmt)

    create_target_table_stmt = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {target_table}
        PARTITIONED BY (ds STRING, h STRING, en STRING)
        ROW FORMAT SERDE '{row_format}'
        STORED AS INPUTFORMAT '{input_fmt}'
        OUTPUTFORMAT '{output_fmt}'
        TBLPROPERTIES ('avro.schema.url'='{full_hdfs_schema_path}')
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


@aac.command('delete_dag')
@click.argument('dag_id', type=str)
def delete_dag(dag_id: str):
    """Delete all references to DAG from Airflow DB."""

    from airflow import settings
    from airflow.jobs import BaseJob
    from airflow.models import (XCom, TaskInstance, TaskFail, SlaMiss, DagRun,
                                DagStat, DagModel)

    session = settings.Session()
    things_deleted = 0
    for model in [XCom, TaskInstance, TaskFail, SlaMiss, BaseJob, DagRun,
                  DagStat, DagModel]:
        for entity in session.query(model).filter(model.dag_id == dag_id).all():
            click.echo(f'Deleting {entity!r}...')
            session.delete(entity)
            things_deleted += 1

    if things_deleted:
        session.commit()
        session.close()
        click.echo(f'Committed {things_deleted} deletions.')
        exit(0)
    else:
        click.echo(f'Nothing to do for dag_id {dag_id!r}.')
        exit(1)


if __name__ == '__main__':
    aac()
