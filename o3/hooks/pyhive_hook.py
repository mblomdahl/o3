# -*- coding: utf-8 -*-
"""
Dumbed-down Hive hook somewhat-inspired-by :py:mod:`airflow.hooks.hive_hooks`,
using :py:class:`pyhive.hive.Connection` backend instead of wrapping the CLI.
"""

from airflow.hooks.base_hook import BaseHook

from pyhive import hive


# noinspection PyAbstractClass
class PyHiveHook(BaseHook):
    """Interact with Hive via PyHive.

    :param str hive_conn_id: Connection ID to fetch connection info.
    :param str username: Effective user for Hive connection.
    :param dict configuration: Hive connection properties (`set <prop>=<val>;`).
    """

    def __init__(self, hive_conn_id: str = 'hiveserver2_default',
                 username: str = None, configuration: dict = None):
        super().__init__(source='hive')
        self.hive_conn_id = hive_conn_id
        self.username = username
        self.configuration = configuration or {
            'hive.exec.dynamic.partition.mode': 'nonstrict'
        }

    def get_conn(self) -> hive.Connection:
        connection = self.get_connections(self.hive_conn_id)[0]

        if self.username:
            username = self.username
        else:
            username = connection.login

        return hive.Connection(host=connection.host, port=connection.port,
                               username=username,
                               configuration=self.configuration)
