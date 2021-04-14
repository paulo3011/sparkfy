from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class MyDummyOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            name: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = "MyDummyOperator Hello {}".format(self.name)
        print(message)
        return message
