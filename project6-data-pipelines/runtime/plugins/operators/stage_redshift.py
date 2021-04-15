from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    The stage operator can load any JSON formatted files
    from S3 to Amazon Redshift. The operator creates and runs a SQL COPY
    statement based on the parameters provided. The operator's parameters
    needs to specify where in S3 the file is loaded and what is the
    target table.

    The parameters should be used to distinguish between JSON file. Another
    important requirement of the stage operator is containing a templated field
    that allows it to load timestamped files from S3 based on the execution
    time and run backfills.

    :param task_id: a unique, meaningful id for the task
    :type task_id: str

    :param source_path: where in S3 the file is loaded
    :type source_path: str

    :param target_table: what is the target table
    :type target_table: str

    :param redshift_conn_id: redshift connection ID
    :type redshift_conn_id: str

    :param partition_by: (Optional) part of the path who is related to
    partition. e.g: the path log_data/2018/11/2018-11-01-events.json
    has the partition: /2018/11/ and source_path: log_data
    :type partition_by: str

    :param jsonpaths: (Optional) a JSONPaths file to parse the JSON
    source data. A JSONPaths file is a text file that contains a
    single JSON object with the name "jsonpaths" paired with an array
    of JSONPath expressions.
    :type jsonpaths: str

    seealso:
    https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html
    https://strftime.org/
    """
    ui_color = '#358140'
    template_fields = ["partition_by"]

    @apply_defaults
    def __init__(self,
                 source_path="",
                 target_table="",
                 redshift_conn_id="",
                 partition_by="/{{execution_date.strftime('%Y/%-m/')}}",
                 jsonpaths=None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        # self.execution_date = execution_date
        self.partition_by = partition_by

    def execute(self, context):
        # self.log.info(f"execution_date: {self.execution_date}")
        self.log.info(f"partition_by: {self.partition_by}")
        self.log.info(f"StageToRedshiftOperator not implemented yet.")
