from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
        Operator to insert data into the fact table
        
        Parameters: 
            redshift_conn_id (String) - connection id for AWS Redshift
            table_name (String) - table name
            sql_query (String) - SQL query to load the data into the table
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id,
                sql_query,
                table_name,
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table_name = table_name

    def execute(self, context):
        redshift = PostgresHook(self.redshift_conn_id)

        self.log.info(f'Loading data into a fact table {self.table_name}')
        insert_query = f"INSERT INTO {self.table_name} {self.sql_query}"
        redshift.run(insert_query)