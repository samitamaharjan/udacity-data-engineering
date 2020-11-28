from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries

# AWS_KEY= os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Samita',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Task to create the redshift tables IF NOT EXISTS.
create_tables = PostgresOperator(
        task_id = 'Create_tables',
        postgres_conn_id ='redshift',
        sql = 'create_tables.sql',
        dag = dag
    )

# Task to load log data into the redshift table - staging_events
stage_events_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_events',
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    table_name = "staging_events",
    s3_bucket = "udacity-dend",
    s3_key = "log_data",
    log_path = "s3://udacity-dend/log_json_path.json",
    dag=dag
)

# Task to load song data into the redshift table - staging_songs
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = 'Stage_songs',
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    table_name = "staging_songs",
    s3_bucket = "udacity-dend",
    s3_key = "song_data",
    dag=dag
)

# Task to insert data into the fact table - songplays
load_songplays_table = LoadFactOperator(
    task_id = 'Load_songplays_fact_table',
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    sql_query = SqlQueries.songplay_table_insert,
    table_name = "songplays",
    dag=dag
)

# Task to insert data into the dimension table - users
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    table_name = "users",
    sql_query = SqlQueries.user_table_insert,
    truncate = True,
    dag=dag
)

# Task to insert data into the dimension table - songs
load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    table_name = "songs",
    sql_query = SqlQueries.song_table_insert,
    truncate = True,
    dag=dag
)

# Task to insert data into the dimensiontable - artists
load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    table_name = "artists",
    sql_query = SqlQueries.artist_table_insert,
    truncate = True,
    dag=dag
)

# Task to insert the data into the dimension table - time
load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    table_name = "time",
    sql_query = SqlQueries.time_table_insert,
    truncate = True,
    dag=dag
)

# Task to check the data quality of the fact and dimension tables
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    aws_credentials_id = "aws_credentials",
    redshift_conn_id = "redshift",
    list_of_tables = ["songplays", "users", "songs", "artists", "time"],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Design a data pipeline using above tasks.
start_operator >> create_tables
create_tables >> [stage_events_to_redshift, stage_songs_to_redshift]
[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
[load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
run_quality_checks >> end_operator