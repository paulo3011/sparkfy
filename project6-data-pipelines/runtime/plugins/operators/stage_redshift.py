from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    The stage operator can load any JSON formatted files
    from S3 to Amazon Redshift. The operator creates and runs a SQL COPY
    statement based on the parameters provided. The operator's parameters
    needs to specify where in S3 the file is loaded and what is the
    target table.

    The stage operator containing a templated field called partition_by
    that allows it to load timestamped files from S3 based on the execution
    time and run backfills.

    :param task_id: a unique, meaningful id for the task
    :type task_id: str

    :param source_path: where in S3 the file is loaded
    :type source_path: str

    :param target_table: what is the target table
    :type target_table: str

    :param conn_id: redshift connection ID
    :type conn_id: str

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
                 conn_id="redshift",
                 iam_role="",
                 partition_by="/{{execution_date.strftime('%Y/%-m/')}}",
                 jsonpaths=None,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.source_path = source_path
        self.target_table = target_table
        self.conn_id = conn_id
        self.iam_role = iam_role
        self.partition_by = partition_by
        self.jsonpaths = jsonpaths

    def execute(self, context):
        self.log.info(f"source_path: {self.source_path}")
        self.log.info(f"target_table: {self.target_table}")
        self.log.info(f"conn_id: {self.conn_id}")
        self.log.info(f"partition_by: {self.partition_by}")
        self.log.info(f"jsonpaths: {self.jsonpaths}")
        self.log.info(f"StageToRedshiftOperator not implemented yet.")
        self._load_data_to_redshift(
            self.source_path,
            self.target_table,
            self.conn_id,
            self.iam_role,
            self.partition_by,
            self.jsonpaths)

    def _get_json_copy_command(self, target_table, source, iam_role, jsonpaths=None, region="us-east-1"):
        """
        Notes:
        Loss of numeric precision: You might lose precision when loading numbers
        from data files in JSON format to a column that is defined as a numeric
        data type. Some floating point values aren't represented exactly in
        computer systems. As a result, data you copy from a JSON file might
        not be rounded as you expect. To avoid a loss of precision seealso:
        https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html
        """
        _jsonpaths = "auto ignorecase"
        _region = ""
        if jsonpaths is not None:
            _jsonpaths = jsonpaths
        if region is not None:
            _region = f"region '{region}'"

        json_copy = ("""
        COPY {} FROM '{}'
        IAM_ROLE '{}'
        JSON '{}'
        {};
        """).format(
            target_table,
            source,
            iam_role,
            _jsonpaths,
            _region
            )
        return json_copy

    def _load_data_to_redshift(self, source_path, target_table, conn_id, iam_role, partition_by=None, jsonpaths=None, *args, **kwargs):
        _source_path = source_path
        if partition_by is not None:
            _source_path = source_path + partition_by

        self.log.info(f"Creating Hook PostgresHook('{conn_id}')")
        redshift_hook = PostgresHook(conn_id)
        sql_command = self._get_json_copy_command(
            target_table, _source_path, iam_role, jsonpaths)
        self.log.info(f"sql_copy: {sql_command}")
        redshift_hook.run(sql_command)
