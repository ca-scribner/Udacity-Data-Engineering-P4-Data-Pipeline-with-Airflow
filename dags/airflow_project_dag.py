from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries
from load_check_subdag import get_load_check_dag

REDSHIFT_CONN_ID = "redshift"
AWS_CREDENTIALS_ID = "aws_credentials"

# Default arguments passed to all operators in the DAG.  Can be overridden at the operators level
default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

# Instantiate the dag
dag = DAG('airflow_project_dag',  # DEBUG rename
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          max_active_runs=1,
          schedule_interval='0 * * * *'
        )


# Define the tasks
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table="staging_events",
    s3_bucket='udacity-dend',
    s3_key="log_data/",
    file_spec="JSON 's3://udacity-dend/log_json_path.json'",
    other_options="COMPUPDATE OFF STATUPDATE OFF"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    conn_id=REDSHIFT_CONN_ID,
    aws_credentials_id=AWS_CREDENTIALS_ID,
    table="staging_songs",
    s3_bucket='udacity-dend',
    s3_key="song_data/",
    file_spec="JSON 'auto'",
    other_options="COMPUPDATE OFF STATUPDATE OFF"
)

table_name = "songplays"
task_id = f"load_check_{table_name}"
load_check_songplays_table = SubDagOperator(
    subdag=get_load_check_dag(
        parent_dag_name=dag.dag_id,
        task_id=task_id,
        conn_id=REDSHIFT_CONN_ID,
        table=table_name,
        sql_select=SqlQueries.songplays_table_insert,
        load_type="fact",
        start_date=default_args['start_date'],
    ),
    task_id=task_id,
    dag=dag,
)

table_name = "users"
task_id = f"load_check_{table_name}"
load_check_users_table = SubDagOperator(
    subdag=get_load_check_dag(
        parent_dag_name=dag.dag_id,
        task_id=task_id,
        conn_id=REDSHIFT_CONN_ID,
        table=table_name,
        sql_select=SqlQueries.users_table_insert,
        load_type="dimension",
        start_date=default_args['start_date'],
    ),
    task_id=task_id,
    dag=dag,
)

table_name = "songs"
task_id = f"load_check_{table_name}"
load_check_songs_table = SubDagOperator(
    subdag=get_load_check_dag(
        parent_dag_name=dag.dag_id,
        task_id=task_id,
        conn_id=REDSHIFT_CONN_ID,
        table=table_name,
        sql_select=SqlQueries.songs_table_insert,
        load_type="dimension",
        start_date=default_args['start_date'],
    ),
    task_id=task_id,
    dag=dag,
)

table_name = "artists"
task_id = f"load_check_{table_name}"
load_check_artists_table = SubDagOperator(
    subdag=get_load_check_dag(
        parent_dag_name=dag.dag_id,
        task_id=task_id,
        conn_id=REDSHIFT_CONN_ID,
        table=table_name,
        sql_select=SqlQueries.artists_table_insert,
        load_type="dimension",
        start_date=default_args['start_date'],
    ),
    task_id=task_id,
    dag=dag,
)

table_name = "times"
task_id = f"load_check_{table_name}"
load_check_times_table = SubDagOperator(
    subdag=get_load_check_dag(
        parent_dag_name=dag.dag_id,
        task_id=task_id,
        conn_id=REDSHIFT_CONN_ID,
        table=table_name,
        sql_select=SqlQueries.times_table_insert,
        load_type="dimension",
        start_date=default_args['start_date'],
    ),
    task_id=task_id,
    dag=dag,
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)


# Task orchestration
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_check_songplays_table 
stage_songs_to_redshift >> load_check_songplays_table

load_check_songplays_table >> load_check_artists_table
load_check_artists_table >> end_operator

load_check_songplays_table >> load_check_users_table
load_check_users_table >> end_operator

load_check_songplays_table >> load_check_songs_table
load_check_songs_table >> end_operator

load_check_songplays_table >> load_check_times_table
load_check_times_table >> end_operator
