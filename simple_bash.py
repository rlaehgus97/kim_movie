from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

with DAG(
    'simple_bash',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=3)
    },
    description='hello world DAG',
    #schedule_interval=timedelta(days=1),
    schedule="10 4 * * *",
    start_date=datetime(2024, 7, 10),
    catchup=True,
    tags=['simple','bash','etl','shop'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators
    task_date = BashOperator(
        task_id='print.date',
        bash_command="""
            # echo "MY_NAME" => 
            echo "date => `date`"
            echo "ds => {{ds}}"
            echo "ds_nodash => {{ds_nodash}}"
            echo "logical_date => {{logical_date}}"
            echo "logical_date => {{logical_date.strftime("%Y-%m-%d %H:%M:%S")}}"
            echo "execution_date => {{execution_date}}"
            echo "execution_date => {{execution_date.strftime("%Y-%m-%d %H:%M:%S")}}"
            echo "next_execution_date => {{next_execution_date.strftime("%Y-%m-%d %H:%M:%S")}}"
            echo "prev_execution_date => {{prev_execution_date.strftime("%Y-%m-%d %H:%M:%S")}}"
            echo "ts => {{ts}}"
        """
    )

    task_copy = BashOperator(
        task_id="copy.log",
        bash_command="""
            mkdir -p ~/data/{{ds_nodash}}
            cp ~/history_{{ds_nodash}}*.log ~/data/{{ds_nodash}}
        """
    )

    task_cut = BashOperator(
        task_id="cut.log", 
        bash_command="""
            echo "cut"
            mkdir -p ~/data/cut/{{ds_nodash}}
            cat ~/data/{{ds_nodash}}/* | cut -d' ' -f1 > ~/data/cut/{{ds_nodash}}/cut.log
        """,
        trigger_rule="all_success"
    )

    task_sort = BashOperator(
        task_id="sort.log",
        bash_command="""
            echo "sort"
            # Generate Directory
            mkdir -p ~/data/sort/{{ds_nodash}}
            # Store sort result file
            cat ~/data/cut/{{ds_nodash}}/cut.log | sort > ~/data/sort/{{ds_nodash}}/sort.log
        """
    )

    task_count = BashOperator(
        task_id="count.log",
        bash_command="""
            echo "count"
            # Repeat
            mkdir -p ~/data/count/{{ds_nodash}}
            cat ~/data/sort/{{ds_nodash}}/sort.log | uniq -c > ~/data/count/{{ds_nodash}}/count.log
        """
    )

    task_err = BashOperator(
        task_id="err.report",
        bash_command="""
            echo "err report"
        """,
        trigger_rule="one_failed"
    )

    task_done = BashOperator(
        task_id="make.done",
        bash_command="""
            DONE_PATH=~/data/done/{{ds_nodash}}
            mkdir -p ${DONE_PATH}
            # mkdir -p ~/data/done/{{ds_nodash}}
            touch ${DONE_PATH}/_DONE
        """
    )

    task_end = EmptyOperator(task_id='end', trigger_rule="all_done")
    task_start = EmptyOperator(task_id='start')

    task_start >> task_date
    task_date >> task_copy 
    task_copy >> task_cut >> task_sort >> task_count >> task_done >> task_end
    task_copy >> task_err >> task_end


