from datetime import datetime, timedelta
from textwrap import dedent
from pprint import pprint

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

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
    def get_data(ds, **kwargs):
        print(ds)
        print(kwargs)
        print("*" * 20)
        print(f"ds_nodash => {kwargs['ds_nodash']}")
        print(f"kwargs type => {type(kwargs)}")
        print("*" * 20)
        from movie.api.call import gen_url, req, get_key, req2list, list2df, save2df
        key = get_key()
        print(f"MOVIE_API_KEY => {key}")
        


    def print_context(ds, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print("::group::All kwargs")
        pprint(kwargs)
        print("::endgroup::")
        print("::group::Context variable ds")
        print(ds)
        print("::endgroup::")
        return "Whatever you return gets printed in the logs"

    run_this = PythonOperator(
            task_id="print_the_context", 
            python_callable=print_context
    )
    
    get_data = PythonOperator(
        task_id="get_data",
        python_callable=get_data
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
    task_start >> run_this >> task_end
