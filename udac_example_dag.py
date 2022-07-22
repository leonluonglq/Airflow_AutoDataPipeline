from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 6, 1),
    'end_date': datetime(2022, 6, 30),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    catchup=False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          #schedule_interval='@hourly',
          catchup=False,
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
# I had error "AttributeError: 'NoneType' object has no attribute 'get_frozen_credentials'" => so I created connection aws_credentials on Airflow web UI 
# I had error "Delimiter not found" => so I add formatting json and resolved.
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='staging_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_prefix="log_data",
    region="us-west-2",
    formatting='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='staging_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_prefix="song_data/A/A/A",
    region="us-west-2",
    formatting='auto'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songplays',
    sql=SqlQueries.songplay_table_insert,
    append_insert=True
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='users',
    sql=SqlQueries.user_table_insert,
    append_insert=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='songs',
    sql=SqlQueries.song_table_insert,
    append_insert=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='artists',
    sql=SqlQueries.artist_table_insert,
    append_insert=True
    
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table='time',
    sql=SqlQueries.time_table_insert,
    append_insert=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    dq_check=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null", 'expected_result':0},
        {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null", 'expected_result':0}
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator

