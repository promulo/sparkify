from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table='',
                 redshift_conn_id='',
                 sql_select_statement='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_select_statement = sql_select_statement

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from {self.table} table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info(f'Populating {self.table} table')
        insert_query = f"""
            INSERT INTO {self.table}
                {self.sql_select_statement}
        """
        redshift.run(insert_query)
