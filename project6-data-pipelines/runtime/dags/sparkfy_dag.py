from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers.sql_queries import SqlQueries


# AWS_KEY = os.environ.get("AWS_KEY")
# AWS_SECRET = os.environ.get("AWS_SECRET")

# [ok] https://classroom.udacity.com/nanodegrees/nd027/parts/45d1c3b1-d87b-4578-a6d0-7e86bb5fea6c/modules/57c3b9d1-4d8b-4afe-bfb4-92cfac622c7f/lessons/4d1d5892-2cab-4456-8b1a-fb2b5fa1488d/concepts/3b78f18c-3a53-40ab-8300-b1fe5208de97

default_args = {
    "owner": "udacity",
    "start_date": datetime(2018, 11, 1),
    "end_date": datetime(2018, 11, 30),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False
}

dag = DAG("sparkfy_dag",
          default_args=default_args,
          description="Load and transform data in Redshift with Airflow.",
          # schedule_interval="0 23 * * *",
          schedule_interval="@monthly",
          catchup=True
        )

start_operator = DummyOperator(task_id="Begin_execution",  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    source_path="s3://moreira-ud/udacity-dend/log_data/",
    target_table="stage_events",
    conn_id="redshift",
    # partition_by="{{execution_date.strftime("%Y/%m")}}/{{execution_date.strftime("%Y")}}-{{execution_date.strftime("%m")}}-{{execution_date.strftime("%d")}}-events.json",
    partition_by='{{execution_date.strftime("%Y/%m")}}/',
    jsonpaths="s3://moreira-ud/udacity-dend/log_json_path.json",
    retries=0
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    source_path="s3://moreira-ud/udacity-dend/song_data/",
    target_table="stage_songs",
    conn_id="redshift",
    partition_by="",
    jsonpaths=None,
    retries=0
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    sql=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id="Load_user_dim_table",
    dag=dag,
    sql=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id="Load_song_dim_table",
    dag=dag,
    sql=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id="Load_artist_dim_table",
    dag=dag,
    sql=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_dim_table",
    dag=dag,
    sql=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag
)

end_operator = DummyOperator(task_id="Stop_execution",  dag=dag)


start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [
    load_song_dimension_table,
    load_user_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
    ] >> run_quality_checks >> end_operator
