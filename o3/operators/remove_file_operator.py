# -*- coding: utf-8 -*-
"""Custom operator for removing a file."""

import os

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from ..hooks.hdfs_hook import HDFSHook


class RemoveFileOperator(BaseOperator):
    """Remove files, locally or in HDFS.

    :param filepath: File path, list of paths, or callable that produces paths.
    :param str fs_type: 'local' or 'hdfs'.
    """
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(self, filepath, fs_type: str = 'local', *args, **kwargs):
        super(RemoveFileOperator, self).__init__(*args, **kwargs)
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

    def execute(self, context) -> list:
        if self.filepath_strs:
            filepaths = self.filepath_strs
        else:
            filepaths = self.filepath_callable(**context)

        if self.fs_type == 'local':
            for filepath in filepaths:
                self.log.info(f'Removing local file {filepath}')
                os.remove(filepath)
        else:
            hdfs = HDFSHook().get_conn()
            for filepath in filepaths:
                self.log.debug(f'Removing HDFS file {filepath}')
                hdfs.rm(filepath)

        if not filepaths:
            raise AirflowException('No files to remove.')

        return filepaths
