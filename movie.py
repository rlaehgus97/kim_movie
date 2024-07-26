from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

def gen_emp(id, rule="all_success"):
    op = EmptyOperator(task_id=id, trigger_rule=rule)
    return op

with DAG(
        'movie',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=2,
    description='get and save movie data',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 24),
    catchup=True,
    tags=['movie', 'db'],
) as dag:

    get_data = BashOperator(
        task_id="to.get",
        bash_command="""
            echo "to.get"

            # READ_PATH="~/data/csv/{{ds_nodash}}/csv.csv"
            # SAVE_PATH="~/data/parquet/"

            # python ~/airflow/py/csv2parquet.py $READ_PATH $SAVE_PATH
            """
    )

    save_data = BashOperator(
        task_id="to.save",
        bash_command="""
            echo "to.save"

            """
    )

    task_end = gen_emp('end', 'all_done')
    task_start = gen_emp('start')

    task_start >> get_data >> save_data >> task_end
