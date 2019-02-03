"""Common utility functions for `o3` project."""

import gzip
import bz2
import os
import hashlib
import typing
import json
import datetime
import random
import time

from fastavro import writer, parse_schema, validate
from fastavro.validation import ValidationError
# And ensure `fastavro` is using C extensions,
# https://stackoverflow.com/a/39304199/1932683


def open_log_file(log_path: str) -> typing.BinaryIO:
    """Return readable file object for plain, gzipped or bzip2'd file."""
    if log_path.endswith('.bz2'):
        return bz2.open(log_path, 'rb')
    elif log_path.endswith('.gz'):
        return gzip.open(log_path, 'rb')
    elif log_path.endswith('.log') or os.path.basename(log_path).startswith(
            'events.log'):
        return open(log_path, 'rb')
    else:
        raise TypeError(f'Unsupported file suffix for {log_path!r}, must be '
                        f'`.log`, `.bz2` or `.gz`.')


def filter_to_percentage(input_path: str, percentage: float,
                         output_path: str) -> dict:
    """Filter out *percentage* unique identifiers from input to output path."""

    print(f'{datetime.datetime.utcnow().isoformat()[:19]}Z '
          f'Filtering out {percentage} % percent of IDs '
          f'from {input_path!r} (PID {os.getpid()})...')

    _MAX_MD5_INT = int('FF' * 16, 16)
    _DROP_THRESHOLD = _MAX_MD5_INT * (percentage / 100.0)

    def _drop_line(id_bytes: bytes) -> bool:
        if percentage <= 0.0:
            return False
        elif percentage >= 100.0:
            return True
        else:
            if int(hashlib.md5(id_bytes).hexdigest(), 16) <= _DROP_THRESHOLD:
                return False
            else:
                return True

    lines_in, lines_ignored, lines_out = 0, 0, 0
    ids_in, ids_out = set(), set()
    with open_log_file(input_path) as input_file:
        if os.path.exists(output_path):
            os.remove(output_path)
        with open(output_path, 'wb') as output_file:
            for line in input_file:
                lines_in += 1
                try:
                    raw_json_data = line.split(b'\t', maxsplit=5)[4]
                    if raw_json_data == b'{"d":}\n':
                        lines_ignored += 1
                        continue

                    id_field = raw_json_data.split(b'"', maxsplit=10)[9]
                    if len(id_field) < 2:
                        raise ValueError(f'Intolerable ID string {id_field!r}.')

                    ids_in.add(id_field)

                    if _drop_line(id_field):
                        continue
                    else:
                        lines_out += 1
                        ids_out.add(id_field)
                        output_file.write(line)

                except (ValueError, IndexError) as parse_err:
                    print(f'{datetime.datetime.utcnow().isoformat()[:19]}Z '
                          f'{parse_err.__class__.__name__}, line {lines_in} in '
                          f'{input_path!r}: {parse_err} / Content: {line!r}')
                    lines_ignored += 1

    print(f'{datetime.datetime.utcnow().isoformat()[:19]}Z '
          f'Wrote {lines_out} lines ({len(ids_out)} IDs) '
          f'of {lines_in} lines ({len(ids_in)} IDs) '
          f'to {output_path!r}, ignoring {lines_ignored} input lines '
          f'(PID {os.getpid()}).')

    return {
        'output_path': os.path.abspath(output_path),
        'metrics': {
            'lines_in': lines_in,
            'lines_ignored': lines_ignored,
            'lines_out': lines_out,
            'ids_in': len(ids_in),
            'ids_out': len(ids_out)
        }
    }


def split_log_by_classifier(log_path: str, classifiers: [str]) -> dict:
    """Splits log file by classifier string, outputs lines in one file for each.
    """

    print(f'{datetime.datetime.utcnow().isoformat()[:19]}Z '
          f'Splitting log file {log_path!r} '
          f'by classifiers {classifiers!r} '
          f'(PID {os.getpid()})...')

    def _get_log_path_for_classifier(classifier_name: str) -> str:
        if log_path.endswith('.bz2') or log_path.endswith('.gz'):
            return f'{os.path.splitext(log_path)[0]}_{classifier_name}'
        else:
            return f'{log_path}_{classifier_name}'

    def _wipe_existing_output_files(output_paths: [str]) -> None:
        for output_path in output_paths:
            if os.path.exists(output_path):
                os.remove(output_path)

    _wipe_existing_output_files(_get_log_path_for_classifier(classifier_name)
                                for classifier_name in classifiers)

    match_strs = [(b'"' + classifier_name.encode() + b'",', classifier_name)
                  for classifier_name in classifiers]

    output_file_handles = {}

    def _append_to_file(classifier_name: str, data: bytes) -> None:
        output_path = _get_log_path_for_classifier(classifier_name)
        if output_path not in output_file_handles:
            output_file_handles[output_path] = open(output_path, 'wb')

        output_file_handles[output_path].write(data)

    lines_in, lines_dropped, lines_out = 0, 0, 0

    with open_log_file(log_path) as log_file:
        for line in log_file:
            lines_in += 1
            try:
                raw_server_date, version, guid, remote_ip, raw_json_data = \
                    line.split(b'\t', maxsplit=5)
                if raw_json_data == b'{"d":}\n':
                    lines_dropped += 1
                    continue
                else:
                    raw_json_data = raw_json_data[:150]

                for match_str, classifier_name in match_strs:
                    if match_str in raw_json_data:
                        lines_out += 1
                        _append_to_file(classifier_name, line)
                        break
                else:
                    raise AssertionError(f'Nothing matches filter string')

            except (ValueError, AssertionError) as parse_err:
                print(f'{parse_err.__class__.__name__}, line {lines_in} in '
                      f'{log_path!r}: {parse_err} / Content: {line!r}')

    for handle in output_file_handles.values():
        handle.close()

    print(f'{datetime.datetime.utcnow().isoformat()[:19]}Z '
          f'Read {lines_in}, dropped {lines_dropped}, '
          f'and wrote {lines_out} lines '
          f'to output paths {output_file_handles.keys()!r} '
          f'(PID {os.getpid()}).')

    output_path_by_classifier = {
        cl_name: os.path.abspath(_get_log_path_for_classifier(cl_name))
        for cl_name in classifiers
        if _get_log_path_for_classifier(cl_name) in output_file_handles
    }

    return {
        'output_path_by_classifier': output_path_by_classifier,
        'metrics': {
            'lines_in': lines_in,
            'lines_dropped': lines_dropped,
            'lines_out': lines_out
        }
    }
