from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from commons.sql_statements import SqlQueries

# Define default arguments for the DAG
default_args = {
    "owner": "udacity",
    "start_date": pendulum.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
}


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="@hourly",
)
def final_project():
    # Create start operator
    start_operator = EmptyOperator(task_id="Begin_execution")

    # Stage events data from S3 to Redshift
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket=Variable.get("s3_bucket"),
        s3_key="log-data",
        json=f"s3://{Variable.get('s3_bucket')}/log_json_path.json",
    )

    # Stage songs data from S3 to Redshift
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket=Variable.get("s3_bucket"),
        s3_key="song-data",
    )

    # Load data into the songplays fact table
    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift_conn_id="redshift",
        table="songplays",
        sql=SqlQueries.songplay_table_insert,
    )

    # Load data into the users dimension table
    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift_conn_id="redshift",
        table="users",
        sql=SqlQueries.user_table_insert,
    )

    # Load data into the songs dimension table
    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift_conn_id="redshift",
        table="songs",
        sql=SqlQueries.song_table_insert,
    )

    # Load data into the artists dimension table
    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift_conn_id="redshift",
        table="artists",
        sql=SqlQueries.artist_table_insert,
    )

    # Load data into the time dimension table
    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift_conn_id="redshift",
        table="time",
        sql=SqlQueries.time_table_insert,
    )

    # Run data quality checks on the loaded tables
    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        redshift_conn_id="redshift",
        tables=["songplays", "users", "songs", "artists", "time"],
    )

    # Create end operator
    end_operator = EmptyOperator(task_id="End_execution")

    # Define task dependencies: Start the DAG
    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    # Stage data before loading the fact table
    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    # Load dimension tables after the fact table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    # Run quality checks after loading all tables
    load_user_dimension_table >> run_quality_checks
    load_song_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    # End the DAG after quality checks
    run_quality_checks >> end_operator


final_project_dag = final_project()
