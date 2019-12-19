import os, sys
import datetime as dt
import pytest
from pytest_mock import mocker

from airflow.models import Connection, TaskInstance
from SQLTemplatedPythonOperator import SQLTemplatedPythonOperator


sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from tests.fixtures import test_dag, mock_conn_obj


def test_operator_connects_and_succeeds(test_dag, mock_conn_obj, mocker):
    from SQLTemplatedPythonOperator.asserts import BaseHook, assert_str_equals
    mocker.patch.object(BaseHook, 'get_connection', return_value=mock_conn_obj)

    task = SQLTemplatedPythonOperator(
        dag=test_dag,
        task_id='unit_STPO',
        sql="SELECT 'MEOW';",
        python_callable=assert_str_equals,
        op_args=['MEOW'],
        provide_context=True)

    ti = TaskInstance(task=task, execution_date=dt.datetime.now())
    task.execute(ti.get_template_context())


def test_operator_fails_correctly(test_dag, mock_conn_obj, mocker):
    from SQLTemplatedPythonOperator.asserts import BaseHook, assert_str_equals
    mocker.patch.object(BaseHook, 'get_connection', return_value=mock_conn_obj)

    task = SQLTemplatedPythonOperator(
        dag=test_dag,
        task_id='unit_STPO',
        sql="SELECT 'MEOW';",
        python_callable=assert_str_equals,
        op_args=['WOOF'],
        provide_context=True)

    ti = TaskInstance(task=task, execution_date=dt.datetime.now())

    with pytest.raises(AssertionError):
        task.execute(ti.get_template_context())


@pytest.mark.skip(reason="Not yet implemented")
def test_operator_templates_correctly(test_dag, mock_conn_obj):
    """ Having some trouble here. The filepath passed to 'sql' should be resolved and templated automatically.
    This is happening on actual production but not in test environment. """
    pass


if __name__ == '__main__':
    pass
