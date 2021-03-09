from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info('Running data quality checks')
        for check in self.checks:
            query = check[0]
            expected = check[1]

            result = redshift.get_records(query)

            try:
                assert result is not None
                assert len(result) == 1
                assert result[0] is not None
                assert len(result[0]) == 1
            except AssertionError:
                raise ValueError(f"Data quality check failed. Query '{query}' returned invalid result set")

            num_records = result[0][0]
            try:
                assert num_records == expected
            except AssertionError:
                raise ValueError(f"Data quality check failed. Query '{query}' should return {expected} but was {num_records}")

        self.log.info("All data quality checks passed")
