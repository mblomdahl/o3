"""Importable constants for `o3` project."""

from datetime import datetime, timedelta

LOG_CLASSIFIERS = [
    'Location', 'Performance', 'BSS', 'BSSEnd', 'AppInfo', 'Connection',
    'Disconnect', 'RxTxData', 'Failure', 'SystemInfo', 'NWDOperation',
    'KPIInfo', 'UserAction', 'QoS', 'RadioManagement',
    'RadioManagementEnd', 'ThroughputSamples', 'CaptivePortalAlert'
]


DEFAULT_ARGS = {
    'owner': 'mblomdahl',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 18),
    'email': ['mats.blomdahl@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(seconds=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}
