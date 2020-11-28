from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
        Operator to insert data into the dimension table
        
        Parameters: 
            redshift_conn_id (String) - connection id for AWS Redshift
            table_name (String) - table name
            sql_query (String) - SQL query to load the data into the table
            truncate (Boolean) - parameter to define either truncate the table first
                                before loading the data or only append the data
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id,
                table_name,
                sql_query,
                truncate=False,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if(self.truncate):
            self.log.info(f"Truncating the table {self.table_name}")
            truncate_query = f"TRUNCATE TABLE {self.table_name}"
            redshift.run(truncate_query)

        self.log.info(f"Loading data into the dimension table {self.table_name}")
        insert_query = f"INSERT INTO {self.table_name} {self.sql_query}"
        redshift.run(insert_query)
