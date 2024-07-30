from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
    PythonVirtualenvOperator,
    is_venv_installed,
)

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

    def branch_fun(ds_nodash):
        # ld = kwargs['ds_nodash']
        import os
        home_dir = os.path.expanduser("~")
        path = os.path.join(home_dir, f"/tmp/test_parquet/load_dt={ds_nodash}")
        if os.path.exists(path):
            return "rm.dir"
        else:
            return "get.data", "echo.task"

    def get_data(ds_nodash):
        #print(ds_nodash) #print(kwargs) #print(f"ds_nodash => {kwargs['ds_nodash']}") <= study how kwargs printed
        from movie.api.call import save2df
        # pip install git+https://github.com/rlaehgus97/kim_mov.git@0.2/api
        # need to run the code above in virtualenv named "air" // without the code above, we can't attain the api key below
        df = save2df(ds_nodash)
        print(df.head(5))

    def save_data(ds_nodash):
        from movie.api.call import apply_type2df

        df = apply_type2df(load_dt=ds_nodash)
        print("*" * 40)
        print(df.head(10))
        print("*" * 40)
        print(df.dtypes)

        # sum of viewers by release date??
        g = df.groupby('openDt')
        sum_df = g.agg({'audiCnt': 'sum'}).reset_index()
        print(sum_df)
        
    branch_op = BranchPythonOperator(
        task_id="branch.op",
        python_callable=branch_fun,
    )

    get_data = PythonVirtualenvOperator(
        task_id="get.data",
        python_callable=get_data,
        # requirements=["git+https://github.com/rlaehgus97/kim_mov.git@fix/0.2.2"], # automatically uninstalled and installed when dag is executed
        requirements=["git+https://github.com/rlaehgus97/kim_mov.git@0.3/api"],
        system_site_packages=False,
        trigger_rule = 'all_done', # 'none_failed_min_one_success' # all_done or none_failed also worked
        #venv_cache_path="/home/dohyun/tmp/airflow_venv/get_data"
    )

    save_data = PythonVirtualenvOperator(
        task_id="save.data",
        python_callable=save_data,
        requirements=["git+https://github.com/rlaehgus97/kim_mov.git@0.3/api"],
        system_site_packages=False,
        trigger_rule = 'one_success', 
        #venv_cache_path="/home/dohyun/tmp/airflow_venv/get_data"
    )

    rm_dir = BashOperator(
        task_id='rm.dir',
        bash_command='rm -rf ~/tmp/test_parquet/load_dt={{ ds_nodash }}',
    )

    echo_task = BashOperator(
        task_id='echo.task',
        bash_command="echo 'task'"
    )

    end = gen_emp('end', 'all_done') #all_done
    start = gen_emp('start')
    join = gen_emp('join', 'all_done') #if we get another data and want to join the tables we got in "save data" task, use another task called join

    start >> branch_op
    start >> join >> save_data

    branch_op >> rm_dir >> get_data
    branch_op >> get_data
    branch_op >> echo_task

    get_data >> save_data >> end

