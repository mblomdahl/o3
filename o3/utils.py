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

