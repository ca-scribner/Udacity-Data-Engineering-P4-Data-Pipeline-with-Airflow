from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id="redshift",
                 sql_quality=None,
                 condition=None,
                 *args, **kwargs):
        """
        Tests quality of Postgres data by checking the results of a SQL query against a condition
        
        Raises if quality test is not met
        
        Args:
            conn_id (str): Airflow connection ID for a Postgres connection
            sql_quality (str): SQL query to be used for quality check (such as "SELECT COUNT(*) FROM tablename")
            condition (function): Function that accepts the object returned by redshift_hook.get_records(sql_quality)
                                  Should raise ValueError if test condition is not met
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        
        if sql_quality is None:
            raise ValueError("Must specify input argument sql_quality")
        else:
            self.sql_quality = sql_quality

        if condition is None:
            raise ValueError("Must specify input argument condition")
        else:
            self.condition = condition

    def execute(self, context):
        """
        Applies the test
        """
        self.log.info(f"Checking quality of {self.table}")
        
        redshift_hook = PostgresHook(self.conn_id)
        results = redshift_hook.get_records(self.sql_quality)
        
        self.condition(results)
        self.log.info(f"Checking quality completed without error")
