from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    The DataQualityOperator operator is the data quality operator,
    which is used to run checks on the data itself.
    The operator's main functionality is to receive one or more SQL
    based test cases along with the expected results and execute the tests.

    For each the test, the test result and expected result needs to be checked
    and if there is no match, the operator should raise an exception and the
    task should retry and fail eventually.

    :param tests: a list of set with tests and conditions to check.
    e.g: tests = tests = [("select 1 as total", "> 0", ">= 1"),
    ("select 2 as total where 1=0", "== 0", ""),
    ("select 2 as total union all select 3 as total", "== 2", "")]
    :type tests: list of set

    :param db_api_hook: database connection db api hook.
    :type db_api_hook: DbApiHook
    """

    ui_color = '#89DA59'
    template_fields = ["tests"]

    @apply_defaults
    def __init__(self,
                 tests,
                 db_api_hook=PostgresHook("redshift"),
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.tests = tests
        self.db_api_hook = db_api_hook
        self.has_error = False
        self.total_errors = 0

    def execute(self, context):
        self.log.info('Starting DataQualityOperator')
        for test in self.tests:
            iterator = iter(test)
            sql_test = next(iterator)
            total_records_expression = next(iterator)
            result_expression = next(iterator)
            self._run_test(sql_test, total_records_expression, result_expression)

        if self.has_error:
            message = f"Data quality check failed. Total tests done: {len(self.tests)}. Total errors: {self.total_errors}"
            raise ValueError(message)

    def _run_test(self, sql_test, total_records_expression="> 0", result_expression="> 0"):
        records = self.db_api_hook.get_records(sql_test)
        self.log.info("records:")
        self.log.info(records)
        total_records = len(records)
        ok_message = f"Data quality check ok. Test done: ({sql_test}) {result_expression}"

        if eval(f"{total_records}{total_records_expression}") is False:
            message = f"Data quality check failed. Were expected that total of records was {total_records_expression}, but got {total_records}. Test done: {sql_test}"
            self._log_test_error(message)
            return

        if result_expression is None or result_expression == "":
            self.log.info(ok_message)
            return

        value = records[0][0]

        if eval(f"{value}{result_expression}") is False:
            message = f"Data quality check failed. Were expected that the value of the first record was {result_expression}, but got {value}. Test done: {sql_test}"
            self._log_test_error(message)
            return

        self.log.info(ok_message)

    def _log_test_error(self, message):
        self.total_errors += 1
        self.has_error = True
        self.log.info(message)
