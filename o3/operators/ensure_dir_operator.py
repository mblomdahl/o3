# -*- coding: utf-8 -*-
"""Custom operator for making sure the dirs we use sensors on actually exist."""

import os

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from ..hooks.hdfs_hook import HDFSHook


class EnsureDirOperator(BaseOperator):
    """Ensures a directory exists, locally or in HDFS.

    :param list paths: One or more directory paths that should exist.
    :param str fs_type: 'local' or 'hdfs'.
    """
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(self, paths: list, fs_type: str = 'local', *args, **kwargs):
        super(EnsureDirOperator, self).__init__(*args, **kwargs)
        if fs_type not in ('local', 'hdfs'):
            raise AirflowException(f'Unsupported fs_type {fs_type!r}.')

        self.paths = paths
        self.fs_type = fs_type

    def execute(self, **_):
        if self.fs_type == 'local':
            for path_ in self.paths:
                if os.path.isdir(path_):
                    self.log.debug(f'Local dir {path_} exists.')
                else:
                    self.log.info(f'Creating local {path_} dir.')
                    os.makedirs(path_)
        else:
            hdfs = HDFSHook().get_conn()
            for path_ in self.paths:
                if hdfs.isdir(path_):
                    self.log.debug(f'HDFS dir {path_} exists.')
                else:
                    self.log.info(f'Creating HDFS {path_} dir.')
                    hdfs.makedirs(path_)
