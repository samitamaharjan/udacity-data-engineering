from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
        Operator to check the data quality
        
        Parameters: 
            redshift_conn_id (String) - connection id for AWS Redshift
            list_of_tables (List) - list of tables 
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id = "",
                list_of_tables = [],
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.list_of_tables = list_of_tables

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        for table in self.list_of_tables:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")
            
            self.log.info(f"Number of records in {table} = {records}")
            if (records is None or len(records[0]) < 1):
                raise ValueError(f"No record found. Data quality check for table - {table} failed! Ugh!!")

            self.log.info(f"Data quality check for table - {table} passed! Yay!!")
