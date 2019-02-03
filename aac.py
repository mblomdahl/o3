#!/usr/bin/env python3

import os
import json
import datetime
import click

from fastavro.read import reader, is_avro

from o3.constants import LOG_CLASSIFIERS
from o3.utils import split_log_by_classifier, filter_to_percentage,\
    convert_to_avro


@click.group()
def aac():
    """Does nothing, goes nowhere. Use sub-commands."""
    pass


@aac.command('filter_logs')
@click.argument('input_path', type=click.Path(exists=True, dir_okay=False))
@click.argument('percentage', type=float)
@click.argument('output_path', type=click.Path(dir_okay=False))
def filter_logs(input_path: str, percentage: float, output_path: str):
    """Filter out percentage of IDs to output path."""

    click.echo(filter_to_percentage(input_path, percentage, output_path))


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
@click.option('-l', '--limit', type=int, help='Max lines to convert')
def to_avro(schema_path: str, log_path: str, output_path: str,
            validate_percentage: float, limit: int):
    """Convert log input to Avro format."""

    click.echo(convert_to_avro(schema_path, log_path,
                               output_path=output_path,
                               delete_existing_avro_file=True,
                               validate_percentage=validate_percentage,
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


if __name__ == '__main__':
    aac()
