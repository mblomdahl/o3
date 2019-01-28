# -*- coding: utf-8 -*-
"""
Copy of :py:mod:`airflow.sensors.hdfs_sensor`, modified to use Py3 compatible
:py:mod:`o3.hooks.hdfs_hook`.
"""

import sys

from airflow import settings
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.log.logging_mixin import LoggingMixin

from ..hooks.hdfs_hook import HDFSHook


class HDFSSensor(BaseSensorOperator):
    """Waits for a file or folder to land in HDFS."""

    template_fields = ('filepath',)
    ui_color = settings.WEB_COLORS['LIGHTBLUE']

    @apply_defaults
    def __init__(self,
                 filepath: str,
                 hdfs_conn_id: str = 'hdfs_default',
                 ignored_ext: list = None,
                 ignore_copying: bool = True,
                 min_file_size: int = None,
                 hook=HDFSHook,
                 *args,
                 **kwargs):
        super(HDFSSensor, self).__init__(*args, **kwargs)
        self.filepath = filepath
        self.hdfs_conn_id = hdfs_conn_id
        self.ignored_ext = ignored_ext or []
        if ignore_copying:
            self.ignored_ext.insert(0, '_COPYING_')
        self.min_file_size = min_file_size
        self.hook = hook

    @staticmethod
    def filter_for_filesize(result: list, min_size: int) -> list:
        log = LoggingMixin().log

        log.debug(f'Filtering result on size >= {min_size} bytes')

        result = [file_ for file_ in result if file_['size'] >= min_size]

        log.debug(f"After size filter result is "
                  f"{list(map(lambda r: r['name'], result))!r}")

        return result

    @staticmethod
    def filter_for_ignored_ext(result: list, ignored_ext: list) -> list:
        log = LoggingMixin().log

        for ext in ignored_ext:
            log.debug(f'Filtering result for ignored extension {ext!r}')

            result = list(filter(
                lambda file_: not file_['name'].rstrip('/').endswith(ext),
                result
            ))

            log.debug(f"After ext filter result is "
                      f"{list(map(lambda r: r['name'], result))!r}")

        return result

    def poke(self, _) -> bool:
        hdfs = self.hook(self.hdfs_conn_id).get_conn()
        self.log.info(f'Poking for file {self.filepath}')

        # noinspection PyBroadException
        try:
            result = hdfs.ls(self.filepath, detail=True)
            self.log.debug(f'Result is {result!r}')

            result = self.filter_for_ignored_ext(result, self.ignored_ext)

            if self.min_file_size:
                result = self.filter_for_filesize(result, self.min_file_size)

            return bool(result)

        except FileNotFoundError:
            self.log.info('File not found.')

        except Exception:
            self.log.error(f'Caught exception: {sys.exc_info()!r}')

        return False
