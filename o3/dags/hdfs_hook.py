# -*- coding: utf-8 -*-
"""
Copy of :py:mod:`airflow.hooks.hdfs_hook`, modified to replace Py3 incompatible
snakebite library wrapping with PyArrow HDFS driver.
"""

from airflow.hooks.base_hook import BaseHook

import pyarrow


# noinspection PyAbstractClass
class HDFSHook(BaseHook):
    """Interact with HDFS. Wrapper around the PyArrow HadoopFileSystem.

    :param str hdfs_conn_id: Connection ID to fetch connection info
    :param str effective_user: Effective user for HDFS operations
    """

    def __init__(self, hdfs_conn_id='hdfs_default', effective_user=None):

        super().__init__(source='hdfs')
        self.hdfs_conn_id = hdfs_conn_id
        self.effective_user = effective_user

    def get_conn(self) -> pyarrow.hdfs.HadoopFileSystem:

        effective_user = self.effective_user

        connection = self.get_connections(self.hdfs_conn_id)[0]

        if not self.effective_user:
            effective_user = connection.login

        return pyarrow.hdfs.connect(host=connection.host, port=connection.port,
                                    user=effective_user, driver='libhdfs3')
