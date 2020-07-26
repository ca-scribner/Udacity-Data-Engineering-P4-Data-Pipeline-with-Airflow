from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


def get_load_check_dag(
    parent_dag_name="",
    task_id="",
    conn_id="",
    table="",
    sql_select="",
    load_type="",
    start_date="",
    *args, **kwargs,
):
    """
    TODO:
    """
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        start_date=start_date,
        **kwargs,
    )
    
    if load_type == "fact":
        load_operator = LoadFactOperator
    elif load_type == "dimension":
        load_operator = LoadDimensionOperator
    else: 
        raise ValueError(f"Unknown load_type {load_type}")
        
    load_task = load_operator(
        task_id=f"load_{table}_{load_type}_table",
        dag=dag,
        conn_id=conn_id,
        table=table,
        sql_select=sql_select
    )

    run_quality_check_task = DataQualityOperator(
        task_id=f'run_data_quality_check_{table}',
        dag=dag,
        sql_quality=f"SELECT COUNT(*) FROM {table}",
        condition=test_contains_rows,
        )
    
    load_task >> run_quality_check_task
    
    return dag
    
def test_contains_rows(results):
    if len(results) < 1 or len(results[0]) < 1:
        raise ValueError("Data quality check failed - no results returned")
    elif results[0][0] < 1:
        raise ValueError("Data quality check failed - results contains 0 rows")

