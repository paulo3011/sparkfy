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
    e.g: tests = [("select count(0) from x", "> 0", "> 1"), ("select count(0) from y", "== 0", "")]
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

    def execute(self, context):
        self.log.info('Starting DataQualityOperator')
        for test in self.tests:
            iterator = iter(test)
            sql_test = next(iterator)
            total_records_expression = next(iterator)
            result_expression = next(iterator)
            self.run_test(sql_test, total_records_expression, result_expression)


    def run_test(self, sql_test, total_records_expression="> 0", result_expression="> 0"):
        records = self.db_api_hook.get_records(sql_test)
        self.log.info("records:")
        self.log.info(records)
        self.db_api_hook.run(sql_test)
        total_records = len(records)
        ok_message = f"Data quality check ok. Test done: {sql_test}"

        if eval(f"{total_records}{total_records_expression}") is False:
            message = f"Data quality check failed. Were expected that total of records was {total_records_expression}, but got {total_records}. Test done: {sql_test}"
            raise ValueError(message)

        if result_expression is None or result_expression == "":
            self.log.info(ok_message)
            return

        value = records[0][0]

        if eval(f"{value}{result_expression}") is False:
            message = f"Data quality check failed. Were expected that the value of the first record was {result_expression}, but got {value}. Test done: {sql_test}"
            raise ValueError(message)

        self.log.info(ok_message)
