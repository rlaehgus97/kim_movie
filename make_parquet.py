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
        'make_parquet',
    default_args={
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    max_active_runs=1,
    max_active_tasks=2,
    description='history log 2 mysql db',
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple', 'bash', 'etl', 'shop', "db", "history", "parquet"],
) as dag:



    task_check = BashOperator(
        task_id="check.done",
        bash_command="""
            DONE_FILE={{ var.value.IMPORT_DONE_PATH }}/{{ds_nodash}}/_DONE
            bash {{ var.value.CHECK_SH }} $DONE_FILE
            """
    )


    task_to_parquet = BashOperator(
        task_id="to.parquet",
        bash_command="""
            echo "to.parquet"

            READ_PATH="~/data/csv/{{ds_nodash}}/csv.csv"
            SAVE_PATH="~/data/parquet/"
            
            
            echo "*******************************"
            echo $SAVE_PATH
            #mkdir -p $SAVE_PATH
            echo $SAVE_PATH
            echo "*******************************"

            python ~/airflow/py/csv2parquet.py $READ_PATH $SAVE_PATH
            """
    )

    task_done = BashOperator(
        task_id="make.done",
        bash_command="""
            echo "make.done"
        """
    )

    task_err = BashOperator(
        task_id="err.report",
        bash_command="""
            echo "err report"
        """,
        trigger_rule="one_failed"
    )

    task_end = gen_emp('end', 'all_done')
    task_start = gen_emp('start')

    task_start >> task_check
    task_check >> task_to_parquet >> task_done >> task_end
    task_check >> task_err >> task_end
