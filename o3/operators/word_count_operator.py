# -*- coding: utf-8 -*-
"""Custom operator for counting words in a file."""

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from ..hooks.hdfs_hook import HDFSHook


class WordCountOperator(BaseOperator):
    """Count words in a file, found locally or in HDFS. Only supports UTF-8.

    :param filepath: File path, list of paths, or callable that produces paths.
    :param str fs_type: 'local' or 'hdfs'.
    """
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(self, filepath, fs_type: str = 'local', *args, **kwargs):
        super(WordCountOperator, self).__init__(*args, **kwargs)
        if fs_type not in ('local', 'hdfs'):
            raise AirflowException(f'Unsupported fs_type {fs_type!r}.')

        if isinstance(filepath, list):
            self.filepath_strs = filepath
        elif isinstance(filepath, str):
            self.filepath_strs = [filepath]
        else:
            self.filepath_strs = None

        self.filepath_callable = filepath if callable(filepath) else None

        self.fs_type = fs_type

    @staticmethod
    def _count_words(text: str) -> str:
        spaces = text.replace('\n', ' ').count(' ')
        return f'counted {spaces + 1} words'

    def execute(self, context) -> list:
        word_counts = []

        if self.filepath_strs:
            filepaths = self.filepath_strs
        else:
            filepaths = self.filepath_callable(**context)

        if self.fs_type == 'local':
            for filepath in filepaths:
                self.log.debug(f'Reading local file {filepath}')
                with open(filepath, 'r', encoding='utf-8') as file_obj:
                    word_counts.append(self._count_words(file_obj.read()))
                    self.log.info(f'Processed local file {filepath}: '
                                  f'{row_counts[-1]}')
        else:
            hdfs = HDFSHook().get_conn()
            for filepath in filepaths:
                self.log.debug(f'Reading HDFS file {filepath}')
                with hdfs.open(filepath, 'rb') as file_obj:
                    word_counts.append(
                        self._count_words(file_obj.read().decode('utf-8')))
                    self.log.info(f'Processed HDFS file {filepath}: '
                                  f'{row_counts[-1]}')

        if not word_counts:
            raise AirflowException('No words counted.')

        return word_counts
