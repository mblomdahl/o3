# -*- coding: utf-8 -*-
"""Custom operator for picking up input file after sensor-poke success."""

import os
import glob
import shutil

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from ..hooks.hdfs_hook import HDFSHook


class MoveFileOperator(BaseOperator):
    """Moves file matching glob from src to dest dir, locally or in HDFS.

    :param str glob_pattern: Glob pattern, e.g. '*.txt'.
    :param str src_dir: Directory path to find file in.
    :param str dest_dir: Directory to move file into.
    :param str fs_type: 'local' or 'hdfs'.
    :param int max_files: Maximum number of files to move.
    """
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(self, glob_pattern: str, src_dir: str, dest_dir: str,
                 fs_type: str = 'local', max_files: int = None,
                 *args, **kwargs):
        super(MoveFileOperator, self).__init__(*args, **kwargs)
        if fs_type not in ('local', 'hdfs'):
            raise AirflowException(f'Unsupported fs_type {fs_type!r}.')

        self.glob_pattern = glob_pattern
        self.src_dir = src_dir.rstrip('/')
        self.dest_dir = dest_dir.rstrip('/')
        self.fs_type = fs_type
        self.max_files = max_files

    def execute(self, **_) -> list:
        dest_paths = []

        if self.fs_type == 'local':
            for src_path in glob.glob(os.path.join(self.src_dir,
                                                   self.glob_pattern)):
                dest_path = os.path.join(self.dest_dir,
                                         os.path.basename(src_path))
                self.log.info(f'Moving local file {src_path} to {dest_path}.')
                shutil.move(src_path, dest_path)
                dest_paths.append(dest_path)
                if self.max_files and len(dest_paths) >= self.max_files:
                    break
        else:
            hdfs = HDFSHook().get_conn()
            for src_path in hdfs.glob(os.path.join(self.src_dir,
                                                   self.glob_pattern)):
                dest_path = os.path.join(self.dest_dir,
                                         os.path.basename(src_path))
                self.log.info(f'Moving HDFS file {src_path} to {dest_path}.')
                hdfs.mv(src_path, dest_path)
                dest_paths.append(dest_path)
                if self.max_files and len(dest_paths) >= self.max_files:
                    break

        if not dest_paths:
            self.log.info('No files found, skipping.')
            raise AirflowSkipException()

        return dest_paths
