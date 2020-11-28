from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
        Operator to load data into the staging table in redshift from S3
        
        Parameters: 
            aws_credentials_id (String) - connectiond id for AWS credentials
            redshift_conn_id (String) - connection id for AWS Redshift
            table_name (String) - table name
            s3_bucket (String) = S3 bucket name,
            s3_key = S3 file path,
            log_path = json log file path,
    """
    
    ui_color = '#358140'

    sql_query = """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    REGION 'us-west-2'
                    FORMAT AS json '{}';
                """

    @apply_defaults
    def __init__(self,
                    aws_credentials_id = "",
                    redshift_conn_id = "",
                    table_name = "",
                    s3_bucket = "",
                    s3_key = "",
                    log_path = "auto",
                    *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_path = log_path

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_file_path = f"s3://{self.s3_bucket}/{self.s3_key}"
        
        self.log.info('Loading data from s3 to redshift')
        query_to_copy = StageToRedshiftOperator.sql_query.format(
            self.table_name,
            s3_file_path,
            credentials.access_key,
            credentials.secret_key,
            self.log_path
        )

        redshift.run(query_to_copy)