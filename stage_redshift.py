from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    #template_fields = ("s3_key",)
    
    sql_copy="""
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            JSON '{}'
            """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_prefix="",
                 region="",
                 formatting = "",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials=aws_credentials
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_prefix=s3_prefix
        self.region=region
        self.formatting = formatting
        
    def execute(self, context):
        #self.log.info('StageToRedshiftOperator not implemented yet')
        self.log.info('StageToRedshiftOperator debug.6 :) ')

        aws_hook=AwsHook(self.aws_credentials)
        self.log.info('aws_hook. ')
        # I had this issue " ERROR - 'NoneType' object has no attribute 'get_frozen_credentials'" => Just a typo in credentials
        aws_credentials=aws_hook.get_credentials()
        self.log.info('get_credentials. ')
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("delete the data from table")
        # I had this issue "psycopg2.ProgrammingError: relation "stage_songs" does not exist" => so I created table manualy in redshift console.
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Data copy from S3 to Redshift")
        #rendered_key=self.s3_key.format(**context)
        s3_path="s3://{}/{}".format(self.s3_bucket, self.s3_prefix)
                                    #rendered_key)
        self.log.info("start sql_copy")
        # "ERROR - Load into table 'staging_songs' failed.  Check 'stl_load_errors' system table for details." => I found a typo wrong s3_prefix
        sql_stmt=StageToRedshiftOperator.sql_copy.format (
            self.table,
            s3_path, 
            aws_credentials.access_key, 
            aws_credentials.secret_key,
            self.region,
            self.formatting
        )

        redshift.run(sql_stmt)




