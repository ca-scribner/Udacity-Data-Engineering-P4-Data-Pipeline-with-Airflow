from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from abc import ABCMeta, abstractmethod


class LoadBaseOperator(BaseOperator, metaclass=ABCMeta):
    """
    Abstract Load*Operator used as a base class for other operators.
    
    Do not instantiate this directly
    """
    @property
    @abstractmethod
    def sql_insert(self):
        """
        Class variable for this specific load method.  Must be specified by subclasses
        """
        pass
    
    @apply_defaults
    def __init__(self,
                 conn_id="redshift",
                 table=None,
                 sql_select=None,
                 *args, **kwargs):
        """
        Instantiates a Load Operator
        
        Args:
            conn_id (str): Airflow connection ID for a Postgres connection
            table (str): Name of the table to load data into
            sql_select (str): SQL select statement that selects data to be loaded
        
        TODO: Add context to sql_select if required
        """        
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        
        if table is None:
            raise ValueError("Must specify value for table")
        else:
            self.table = table
        
        if sql_select is None:
            raise ValueError("Must specify value for sql_select")
        else:
            self.sql_select = sql_select

    def execute(self, context):
        """
        Execute the load of data
        """
        self.log.info(f"Loading {self.table}")
        
        redshift_hook = PostgresHook(self.conn_id)
        query = self.sql_insert.format(table=self.table, sql_select=self.sql_select)
        redshift_hook.run(query)
        self.log.info(f"Loading {self.table} completed without error")

class LoadDimensionOperator(LoadBaseOperator):
    """
    Load operator for dimensions.  Operator clears table before load.
    """
    ui_color = '#80BD9E'
    sql_insert = "DELETE FROM {table}; INSERT INTO {table} {sql_select}"

class LoadFactOperator(LoadBaseOperator):
    """
    Load operator for facts.  Operator appends data directly to existing table.
    """
    ui_color = '#F98866'
    sql_insert = "INSERT INTO {table} {sql_select}"
