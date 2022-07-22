from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql ="",
                 #append_insert=True,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        #self.append_insert=append_insert 

    def execute(self, context):
        self.log.info('LoadFactOperator debug 1')
        
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = f"INSERT INTO {self.table} ({self.sql})"
        self.log.info(self.sql)
        self.log.info(f"QUERY: {formatted_sql}")
        
        redshift.run(formatted_sql)
