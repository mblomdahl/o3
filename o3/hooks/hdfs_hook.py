# -*- coding: utf-8 -*-
"""
Copy of :py:mod:`airflow.hooks.hdfs_hook`, modified to replace Py3 incompatible
snakebite library wrapping with libhdfs3 driver.
"""

from airflow.hooks.base_hook import BaseHook

import hdfs3


# noinspection PyAbstractClass
class HDFSHook(BaseHook):
    """Interact with HDFS. Wrapper around the hdfs3.HDFileSystem.

    :param str hdfs_conn_id: Connection ID to fetch connection info
    :param str effective_user: Effective user for HDFS operations
    """

    def __init__(self, hdfs_conn_id='hdfs_default', effective_user=None):

        super().__init__(source='hdfs')
        self.hdfs_conn_id = hdfs_conn_id
        self.effective_user = effective_user

    def get_conn(self) -> hdfs3.core.HDFileSystem:

        effective_user = self.effective_user

        connection = self.get_connections(self.hdfs_conn_id)[0]

        if not self.effective_user:
            effective_user = connection.login

        return hdfs3.HDFileSystem(host=connection.host, port=connection.port,
                                  user=effective_user)
