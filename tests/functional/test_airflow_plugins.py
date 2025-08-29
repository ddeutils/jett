from jett.plugins.airflow.operators import JettOperator


def test_jett_operator():
    task = JettOperator(task_id="test", tool="demo")
    print(task)
