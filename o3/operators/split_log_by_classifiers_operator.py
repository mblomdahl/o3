# -*- coding: utf-8 -*-
"""Custom operator for splitting a log file."""

import os

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from o3.utils import split_log_by_classifier


class SplitLogByClassifiersOperator(BaseOperator):
    """Splits log lines by a list of a classifiers.

    :param list classifiers: Classifiers expected to be matchable by line.
    :param src_filepath: Source file path or callable that produces one
                         (templated).
    :param bool remove_src: Remove input file after splitting.
    """
    template_fields = ['src_filepath_str']
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(self, classifiers: list, src_filepath, remove_src: bool = True,
                 *args, **kwargs):
        super(SplitLogByClassifiersOperator, self).__init__(*args, **kwargs)

        self.classifiers = classifiers

        if isinstance(src_filepath, str):
            self.src_filepath_str = src_filepath
            self.src_filepath_callable = None
        elif callable(src_filepath):
            self.src_filepath_str = None
            self.src_filepath_callable = src_filepath
        else:
            raise AirflowException(
                f'Incompatible src_filepath {src_filepath!r}.')

        self.remove_src = remove_src

    def execute(self, context) -> dict:
        src_filepath = self.src_filepath_str or self.src_filepath_callable(
            **context)

        splitting_output = split_log_by_classifier(src_filepath,
                                                   self.classifiers)

        if self.remove_src:
            self.log.info(f'Removing {src_filepath}.')
            os.remove(src_filepath)

        if not splitting_output['metrics']['lines_out']:
            self.log.info('No output generated, skipping.')
            raise AirflowSkipException()

        return splitting_output
