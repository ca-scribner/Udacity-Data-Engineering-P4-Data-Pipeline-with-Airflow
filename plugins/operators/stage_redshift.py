from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    sql_copy = """
        COPY {table}
        FROM '{s3_location}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_access_key}'
        {file_spec}
        {other_options}
    """
    
    sql_delete = """
        DELETE FROM {table}
    """
    
    @apply_defaults
    def __init__(self,
                 conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table=None,
                 s3_bucket=None,
                 s3_key=None,
                 file_spec="JSON 'auto'",
                 other_options='',
                 *args, **kwargs):
        """
        Stages a file in S3 to a table in a Postgres instance
        
        Args:
            conn_id (str): Airflow connection ID for a Postgres connection
            aws_credentials_id (str): Airflow connection ID with AWS credentials for S3
            table (str): Name of the table to stage the file to
            s3_bucket (str): S3 bucket with the source data
            s3_key (str): S3 object key for the source data
            file_spec (str): File specifications for the COPY query
            other_options (str): (OPTIONAL) Other arguments passed to COPY query
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        
        if table is None:
            raise ValueError("Must specify target table")
        else:
            self.table = table
        
        if s3_bucket is None:
            raise ValueError("Must specify a source s3_bucket")
        else:
            self.s3_bucket = s3_bucket
        
        if s3_key is None:
            raise ValueError("Must specify a source s3_key")
        else:
            self.s3_key = s3_key
            
        self.file_spec = file_spec
        self.other_options = other_options


    def execute(self, context):
        """
        Executes the staging
        
        Note: To run staging in parallel (eg: for different date ranges), target table must be 
        uniquely named by calling dag
        """
        s3_location = f's3://{self.s3_bucket}/{self.s3_key}'
        self.log.info(f"Staging {self.table} from {s3_location}")

        # Clear existing table
        redshift_hook = PostgresHook(self.conn_id)
        redshift_hook.run(self.sql_delete.format(table=self.table))
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # Stage data
        query = self.sql_copy.format(
            table=self.table,
            s3_location=s3_location,
            access_key=credentials.access_key,
            secret_access_key=credentials.secret_key,
            file_spec=self.file_spec,
            other_options=self.other_options,
        )
        redshift_hook.run(query)
        self.log.info(f"Staging {self.table} completed without error")
