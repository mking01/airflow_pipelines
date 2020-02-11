from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators import LoadDimensionOperator
from helpers import SqlQueries

def load_dimensions_dag(
        parent_name,
        task_id,
        redshift_conn_id,
        table,
        sql_query,
        *args, **kwargs):
    dag = DAG(
        f"{parent_name}.{task_id}",
        **kwargs
    )
    
    create_dim_tables_task = LoadDimensionOperator(
        task_id=f"load_{table}_dim_table",
        dag=dag,
        table=table,
        redshift_conn_id=redshift_conn_id,
        sql_query=sql_query
    )

    create_dim_tables_task  
    return dag
    
    