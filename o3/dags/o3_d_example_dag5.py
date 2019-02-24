"""
### `o3_d_example_dag5` -- A DAG to Clean Up Airflow Logs

Every day:

1. Finds and deletes all files in `$AIRFLOW_HOME/logs/` dir
2. Finds and deletes all empty directories under `$AIRFLOW_HOME/logs/` dir


"""

from airflow import DAG, conf
from airflow.operators.bash_operator import BashOperator

from o3.constants import DEFAULT_ARGS


with DAG('o3_d_example_dag5',
         default_args=DEFAULT_ARGS,
         schedule_interval='@daily',
         catchup=False) as _dag:
    delete_old_log_files = BashOperator(
        task_id='o3_t_delete_old_log_files',
        bash_command=(f'cd {conf.AIRFLOW_HOME} && echo "Deleting files:" && '
                      f'find logs -type f -mtime +14 && '
                      f'find logs -type f -mtime +14 -delete')
    )

    delete_empty_log_dirs = BashOperator(
        task_id='o3_t_delete_empty_log_dirs',
        bash_command=(f'cd {conf.AIRFLOW_HOME} && echo "Deleting dirs:" && '
                      f'find logs -type d -empty && '
                      f'find logs -type d -empty -delete')
    )

    delete_old_log_files >> delete_empty_log_dirs


_dag.doc_md = __doc__
