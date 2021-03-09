from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table='',
                 redshift_conn_id='',
                 sql_select_statement='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_select_statement = sql_select_statement

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f'Populating {self.table} fact table')
        insert_query = f"""
            INSERT INTO {self.table}
                {self.sql_select_statement}
        """
        redshift.run(insert_query)
