from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    insert_sql = """
    INSERT INTO
    {table} 
    {sql}
    """

    truncate_sql = """
    TRUNCATE TABLE {table}
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                # Define your operators params (with defaults) here
                # Example:
                # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql="",
                 truncate=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate = truncate

    def execute(self, context):
          """
         Using truncate-insert pattern to load dimension tables. 
        
         Parameters:
         
         redshift_conn_id:string
             airflow connection to redshift cluster
         table: string
             tables located in redshift cluster
         sql: string
             SQL command to insert values
         truncate: boolean
             Flag to truncate table prior to load
         """



        try:
            redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
            self.log.info("Airflow connection to redshift cluster")
        except AirflowError as e:
            self.log.error(e)

        # Truncate table
        if self.truncate:
            self.log.info(f"Truncate tables {self.table}")
            redshift.run(LoadDimensionOperator.truncate_sql.format(table=self.table))

        # Inserte data 
        self.log.info(f"Insert values on {self.table}")
        redshift.run(LoadDimensionOperator.insert_sql.format(table=self.table, sql=self.sql))

