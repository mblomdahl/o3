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
    :param str src_fs_type: Source file system, 'local' or 'hdfs'.
    :param str dest_fs_type: Destination file system, 'local' or 'hdfs'.
    :param int max_files: Maximum number of files to move.
    """
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(self, glob_pattern: str, src_dir: str, dest_dir: str,
                 src_fs_type: str = 'local', dest_fs_type: str = 'local',
                 max_files: int = None, *args, **kwargs):
        super(MoveFileOperator, self).__init__(*args, **kwargs)

        self.glob_pattern = glob_pattern
        self.src_dir = src_dir.rstrip('/')
        self.dest_dir = dest_dir.rstrip('/')

        if src_fs_type not in ('local', 'hdfs'):
            raise AirflowException(f'Unsupported src_fs_type {src_fs_type!r}.')
        else:
            self.src_fs_type = src_fs_type

        if dest_fs_type not in ('local', 'hdfs'):
            raise AirflowException(
                f'Unsupported dest_fs_type {dest_fs_type!r}.')
        else:
            self.dest_fs_type = dest_fs_type

        self.max_files = max_files

    def execute(self, **_) -> list:
        src_dir_glob = os.path.join(self.src_dir, self.glob_pattern)
        dest_paths = []

        def _get_dest_path(src: str) -> str:
            return os.path.join(self.dest_dir, os.path.basename(src))

        if self.src_fs_type == 'local' and self.dest_fs_type == 'local':
            for src_path in glob.glob(src_dir_glob):
                dest_path = _get_dest_path(src_path)
                self.log.info(f'Moving local file {src_path} to {dest_path}.')
                shutil.move(src_path, dest_path)
                dest_paths.append(dest_path)
                if self.max_files and len(dest_paths) >= self.max_files:
                    break

        elif self.src_fs_type == 'local' and self.dest_fs_type == 'hdfs':
            hdfs = HDFSHook().get_conn()
            for src_path in glob.glob(src_dir_glob):
                dest_path = _get_dest_path(src_path)
                self.log.info(f'Moving local {src_path} to HDFS {dest_path}.')
                hdfs.put(src_path, dest_path)
                os.remove(src_path)
                dest_paths.append(dest_path)
                if self.max_files and len(dest_paths) >= self.max_files:
                    break

        elif self.src_fs_type == 'hdfs' and self.dest_fs_type == 'local':
            raise NotImplementedError('Support for HDFS -> local move pending.')

        elif self.src_fs_type == 'hdfs' and self.dest_fs_type == 'hdfs':
            hdfs = HDFSHook().get_conn()
            for src_path in hdfs.glob(src_dir_glob):
                dest_path = _get_dest_path(src_path)
                self.log.info(f'Moving HDFS file {src_path} to {dest_path}.')
                hdfs.mv(src_path, dest_path)
                dest_paths.append(dest_path)
                if self.max_files and len(dest_paths) >= self.max_files:
                    break

        if not dest_paths:
            self.log.info('No files found, skipping.')
            raise AirflowSkipException()

        return dest_paths
