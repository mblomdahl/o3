# -*- coding: utf-8 -*-
"""Custom operator for converting a log file to Avro format."""

import os

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from o3.utils import convert_to_avro


class ConvertLogToAvroOperator(BaseOperator):
    """Converts input log file to Avro.

    :param str avro_schema_path: Local file path for Avro schema.
    :param src_filepath: Local file path or callable that produces one.
    :param float validate_percentage: Output percentage to run validation on.
    :param bool remove_src: Remove input file after conversion.
    """
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(self, avro_schema_path: str, src_filepath,
                 validate_percentage: float = 100.0,
                 remove_src: bool = True,
                 *args, **kwargs):
        super(ConvertLogToAvroOperator, self).__init__(*args, **kwargs)

        self.avro_schema_path = avro_schema_path

        if isinstance(src_filepath, str):
            self.src_filepath_str = src_filepath
            self.src_filepath_callable = None
        elif callable(src_filepath):
            self.src_filepath_str = None
            self.src_filepath_callable = src_filepath
        else:
            raise AirflowException(
                f'Incompatible src_filepath {src_filepath!r}.')

        if 0.0 <= validate_percentage <= 100.0:
            self.validate_percentage = validate_percentage
        else:
            raise AirflowException(
                f'Out-of-range percentage {validate_percentage!r}.')

        self.remove_src = remove_src

    def execute(self, context) -> dict:
        src_filepath = self.src_filepath_str or self.src_filepath_callable(
            **context)

        if not src_filepath:
            self.log.info('No filepath found, skipping.')
            raise AirflowSkipException()

        self.log.info(f'Converting local {src_filepath} to Avro...')
        conversion_output = convert_to_avro(
            self.avro_schema_path, src_filepath,
            validate_percentage=self.validate_percentage
        )

        if self.remove_src:
            self.log.info(f'Removing {src_filepath}.')
            os.remove(src_filepath)

        return conversion_output
