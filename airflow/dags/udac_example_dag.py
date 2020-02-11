#/opt/airflow/start.sh

from datetime import datetime, timedelta
from airflow import DAG
import os
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from udac_dimensions_subdag import load_dimensions_dag
from airflow.models import BaseOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                DataQualityOperator)
from helpers import SqlQueries

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

# Generate start date and time
start_date = datetime.utcnow()

default_args = {
                'owner': 'udacity',
                'start_date': start_date,
                'depends_on_past': False,
                'retries': 3,
                'retry_delay': timedelta(minutes=5),
                'email_on_retry': False,
                'catchup': False
            }

dag = DAG("udac_example_dag",
          default_args=default_args,
          description="Load and transform data in Redshift with Airflow",
          schedule_interval=timedelta(minutes=60)
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
                                                    task_id="Stage_events",
                                                    dag=dag,
                                                    redshift_conn_id="redshift",
                                                    aws_credentials_id="aws_credentials",
                                                    table="events",
                                                    s3_bucket="udacity-dend",
                                                    s3_key="log_data",
                                                    provide_context=True,
                                                    execution_date=start_date
                                                )

stage_songs_to_redshift = StageToRedshiftOperator(
                                                task_id="Stage_songs",
                                                dag=dag,
                                                redshift_conn_id="redshift",
                                                aws_credentials_id="aws_credentials",
                                                table="songs",
                                                s3_bucket="udacity-dend",
                                                s3_key="song_data",
                                                provide_context=True,
                                                execution_date=start_date
                                            )

load_songplays_table = LoadFactOperator(
                                        task_id="Load_songplays_fact_table",
                                        dag=dag,
                                        redshift_conn_id="redshift",
                                        provide_context=True,
                                        sql_query=SqlQueries.songplay_table_insert
                                    )

    
load_user_dim_task="load_user_dim_table"
load_user_dimension_table = SubDagOperator(
                                subdag = load_dimensions_dag(
                                    parent_name="udac_example_dag",
                                    task_id = load_user_dim_task,
                                    redshift_conn_id = "redshift",
                                    table = "users",
                                    sql_query=SqlQueries.user_table_insert,
                                    start_date=start_date
                                ),
                                dag=dag,
                                task_id=load_user_dim_task,
                            )

load_song_dim_task="load_song_dim_table"
load_song_dimension_table  = SubDagOperator(
                                subdag = load_dimensions_dag(
                                    parent_name="udac_example_dag",
                                    task_id=load_song_dim_task,
                                    redshift_conn_id="redshift",
                                    table="users",
                                    sql_query=SqlQueries.song_table_insert,
                                    start_date=start_date
                                ),
                                dag=dag,
                                task_id=load_song_dim_task,
                            )

load_artist_dim_task="load_artist_dim_table"
load_artist_dimension_table  = SubDagOperator(
                                subdag=load_dimensions_dag(
                                    parent_name="udac_example_dag",
                                    task_id=load_artist_dim_task,
                                    redshift_conn_id="redshift",
                                    table="users",
                                    sql_query=SqlQueries.artist_table_insert,
                                    start_date=start_date
                                ),
                                dag=dag,
                                task_id=load_artist_dim_task,
                            )

load_time_dim_task="load_time_dim_table"
load_time_dimension_table = SubDagOperator(
                                subdag=load_dimensions_dag(
                                    parent_name="udac_example_dag",
                                    task_id=load_time_dim_task,
                                    redshift_conn_id="redshift",
                                    table="users",
                                    sql_query=SqlQueries.time_table_insert,
                                    start_date=start_date
                                ),
                                dag=dag,
                                task_id=load_time_dim_task,
                            )

run_quality_checks = DataQualityOperator(
                                        dag=dag,
                                        task_id="Run_data_quality_checks",
                                        redshift_conn_id="redshift",
                                        tables=["users","songs","time","songplay","artist"],
                                        provide_context=True
                                    )

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)


start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator