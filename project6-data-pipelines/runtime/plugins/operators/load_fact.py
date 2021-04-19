from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 sql,
                 db_api_hook=PostgresHook("redshift"),
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.sql = sql
        self.db_api_hook = db_api_hook

    def execute(self, context):
        self.log.info('Starting LoadFactOperator')
        self.db_api_hook.run(self.sql)
