import os, sys
import datetime as dt
import pytest

from airflow import AirflowException
from airflow.models import DAG, Connection, TaskInstance
from airflow.hooks.base_hook import BaseHook
from RedshiftTableConstraintOperator import RedshiftTableConstraintOperator

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from tests.fixtures import test_dag, mock_conn_obj


def test_operator_is_valid(test_dag, mocker, mock_conn_obj):

    mocker.patch.object(BaseHook, 'get_connection', return_value=mock_conn_obj)

    task = RedshiftTableConstraintOperator(
        dag=test_dag,
        task_id='foo',
        schema='my_schema',
        table='my_booking')

    ti = TaskInstance(task=task, execution_date=dt.datetime.now())
    task.execute(ti.get_template_context())


def test_operator_table_doesnt_exist(test_dag, mocker, mock_conn_obj):

    mocker.patch.object(BaseHook, 'get_connection', return_value=mock_conn_obj)

    task = RedshiftTableConstraintOperator(
        dag=test_dag,
        task_id='bar',
        schema='analytics',
        table='MEOW')

    ti = TaskInstance(task=task, execution_date=dt.datetime.now())
    with pytest.raises(AirflowException):
        task.execute(ti.get_template_context())


if __name__ == '__main__':
    pass
