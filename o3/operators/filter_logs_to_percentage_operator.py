# -*- coding: utf-8 -*-
"""Custom operator for filtering out a percentage of input log files."""

import os
import glob

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from o3.utils import filter_to_percentage


class FilterLogsToPercentageOperator(BaseOperator):
    """Filters input files to a percentage, from src dir glob match to dest dir.

    :param float percentage: Percentage of input to keep, e.g. `3.0` for 3 %.
    :param str glob_pattern: Glob pattern, e.g. '*.log' (templated).
    :param str src_dir: Directory path to find input in.
    :param str dest_dir: Directory to write filtered output to.
    :param str src_fs_type: Source file system, only 'local' supported.
    :param str dest_fs_type: Destination file system, only 'local' supported.
    :param list match_strs: Optionally pre-filter input lines by
                            one or more exact strings.
    :param int max_files: Maximum number of files to filter.
    :param bool remove_src: Remove input file after filtering.
    """
    template_fields = ['glob_pattern']
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(self, percentage: float,
                 glob_pattern: str, src_dir: str, dest_dir: str,
                 src_fs_type: str = 'local', dest_fs_type: str = 'local',
                 match_strs: list = None, max_files: int = None,
                 remove_src: bool = True, *args, **kwargs):
        super(FilterLogsToPercentageOperator, self).__init__(*args, **kwargs)

        if 0.0 <= percentage <= 100.0:
            self.percentage = percentage
        else:
            raise AirflowException(f'Out-of-range percentage {percentage!r}.')

        self.glob_pattern = glob_pattern
        self.match_strs = match_strs
        self.src_dir = src_dir.rstrip('/')
        self.dest_dir = dest_dir.rstrip('/')

        if src_fs_type != 'local':
            raise AirflowException(f'Unsupported src_fs_type {src_fs_type!r}.')
        else:
            self.src_fs_type = src_fs_type

        if dest_fs_type != 'local':
            raise AirflowException(
                f'Unsupported dest_fs_type {dest_fs_type!r}.')
        else:
            self.dest_fs_type = dest_fs_type

        self.max_files = max_files
        self.remove_src = remove_src

    def execute(self, **_) -> list:
        src_dir_glob = os.path.join(self.src_dir, self.glob_pattern)
        dest_paths = []

        def _get_dest_path(src: str) -> str:
            if src.lower().endswith('.bz2') or src.lower().endswith('.gz'):
                basename = os.path.splitext(os.path.basename(src))[0]
            else:
                basename = os.path.basename(src)
            return os.path.join(self.dest_dir,
                                f'{basename}__'
                                f'{round(self.percentage, 1)}pct_filtered')

        if self.src_fs_type == 'local' and self.dest_fs_type == 'local':
            for src_path in glob.glob(src_dir_glob):
                dest_path = _get_dest_path(src_path)
                self.log.info(f'Filtering local {src_path} to {dest_path}...')
                filter_to_percentage(src_path, self.percentage, dest_path,
                                     prefilter_by=self.match_strs)
                dest_paths.append(dest_path)
                if self.remove_src:
                    self.log.info(f'Removing {src_path}.')
                    os.remove(src_path)
                if self.max_files and len(dest_paths) >= self.max_files:
                    break

        if not dest_paths:
            self.log.info('No files found, skipping.')
            raise AirflowSkipException()

        return dest_paths
