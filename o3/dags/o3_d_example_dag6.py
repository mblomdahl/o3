"""
### `o3_d_example_dag6` -- A DAG to Fail and Send an Email About It


"""
from datetime import timedelta

from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.bash_operator import BashOperator

from o3.constants import DEFAULT_ARGS


with DAG('o3_d_example_dag6',
         default_args=DEFAULT_ARGS,
         schedule_interval=None,
         catchup=False) as _dag:
    send_email_about_starting_job = EmailOperator(
        task_id='o3_t_email_about_starting_job',
        to='mats.blomdahl@gmail.com',
        subject='Airflow about to start job...',
        html_content='<h1>Aha!</h1>'
    )

    break_sla = BashOperator(
        task_id='o3_t_breaking_the_10s_sla',
        sla=timedelta(seconds=10),
        bash_command='sleep 30'
    )

    send_email_about_starting_job >> break_sla


_dag.doc_md = __doc__
