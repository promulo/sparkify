from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 table="",
                 aws_credentials_id="",
                 redshift_conn_id="",
                 s3_data_location="",
                 aws_region="us-west-2",
                 json_path_location='auto',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_data_location = s3_data_location
        self.aws_region = aws_region
        self.json_path = json_path_location

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Clearing data from {self.table} table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info(f"Copying source data from {self.s3_data_location} into {self.table} table")
        copy_query = f"""
            COPY {self.table}
            FROM '{self.s3_data_location}'
            ACCESS_KEY_ID '{credentials.access_key}'
            SECRET_ACCESS_KEY '{credentials.secret_key}'
            COMPUPDATE OFF REGION '{self.aws_region}'
            JSON '{self.json_path}'
        """
        redshift.run(copy_query)
