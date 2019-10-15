from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries
from airflow.operators.subdag_operator import SubDagOperator
from subdag.subdag import get_dimension_load_dag
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)

default_args = {
    'owner': 'st',
    'start_date': datetime(2019, 9, 12),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False,
    'schedule_interval': '@daily'
}

dag = DAG(
    'etl_task',
    default_args=default_args,
    description='ETL in Redshift with Airflow'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="stage_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket='udacity-dend',
    s3_key="log_data",
    file_format_info="format as json 's3://udacity-dend/log_json_path.json'",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='stage_songs',
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket='udacity-dend',
    s3_key="song_data",
    file_format_info="json 'auto'",   
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='load_songplays_fact_table',
    redshift_conn_id="redshift",
    table="songplays",
    sql_stmt=SqlQueries.songplay_table_insert,
    dag=dag
)

load_dimensions_subtask = SubDagOperator(
    subdag=get_dimension_load_dag(
        parent_dag_name="etl_task",
        task_id="load_dimensions_subdag",
        redshift_conn_id="redshift",
        tables=({'table':'users', 'sql':SqlQueries.user_table_insert},
              {'table':'songs', 'sql':SqlQueries.song_table_insert},
              {'table':'artists', 'sql':SqlQueries.artist_table_insert},
              {'table':'time', 'sql':SqlQueries.time_table_insert}),
        start_date=datetime(2019, 9, 12),
    ),
    task_id="load_dimensions_subdag",
    dag=dag,
)


run_quality_checks = DataQualityOperator(
    task_id='run_data_quality_checks',
    redshift_conn_id="redshift",
    table="time",
    dag=dag
)

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)



start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
load_songplays_table >> load_dimensions_subtask
load_dimensions_subtask >> run_quality_checks
run_quality_checks >> end_operator



