from custom_operator.hello_operator import HelloOperator

with dag:
    hello_task = HelloOperator(task_id='sample-task', name='foo_bar')