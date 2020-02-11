from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                sql_query="",
                table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query,
        self.table=table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        query = self.sql_query.format(self.table)
        redshift_hook.run(str(query))
        self.log.info(f'Job Success: Creating {table} dimension table.')
