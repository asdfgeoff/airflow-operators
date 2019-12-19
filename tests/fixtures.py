import os
import datetime as dt
import pytest
from airflow.models import DAG, Connection


@pytest.fixture
def test_dag():
    DIR = os.path.dirname(os.path.abspath(__file__))

    return DAG(
        'test_dag',
        default_args={'owner': 'airflow', 'start_date': dt.datetime(2018, 1, 1)},
        template_searchpath=DIR,
        schedule_interval=dt.timedelta(days=1)
    )


@pytest.fixture
def mock_conn_obj():
    return Connection(conn_type='postgres',
                      host=os.environ['REDSHIFT_HOST'],
                      login=os.environ['REDSHIFT_USERNAME'],
                      password=os.environ['REDSHIFT_PASSWORD'],
                      port=os.environ['REDSHIFT_PORT'],
                      schema=os.environ['REDSHIFT_DB'])


if __name__ == '__main__':
    pass

