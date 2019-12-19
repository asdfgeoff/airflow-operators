import pandas as pd
from airflow.hooks.base_hook import BaseHook

REDSHIFT_CONNECTION_NAME = 'redshift_etl'  # Change this to reflect your own connection name


def get_pandas_connection_string(conn_name=REDSHIFT_CONNECTION_NAME):
    """ Reads airflow connection and returns an sqlalchemy-style connection string suitable for pd.read_sql() function. """
    conn = BaseHook.get_connection(REDSHIFT_CONNECTION_NAME)
    template = 'postgresql://{db_user}:{db_pass}@{host}:{port}/{db}'
    return template.format(db_user=conn.login,
                           db_pass=conn.get_password(),
                           host=conn.host,
                           port=conn.port,
                           db=conn.schema)


def assert_zero(**kwargs):
    query = kwargs['templates_dict']['sql']
    result = pd.read_sql(query, get_pandas_connection_string()).iloc[0, 0]
    assert result == 0


def assert_str_equals(x, **kwargs):
    query = kwargs['templates_dict']['sql']
    result = pd.read_sql(query, get_pandas_connection_string()).iloc[0, 0]
    assert result == x


def assert_pct_greater_than(pct, **kwargs):
    query = kwargs['templates_dict']['sql']
    result = pd.read_sql(query, get_pandas_connection_string()).iloc[0, 0]
    assert result > pct, 'Observed value of {:.6f} is lower than set threshold of {:.6f}'.format(result, pct)
    print('Observed value: {:.6f}\nThreshold: {:.6f}'.format(result, pct))


def assert_pct_less_than(pct, **kwargs):
    query = kwargs['templates_dict']['sql']
    result = pd.read_sql(query, get_pandas_connection_string()).iloc[0, 0]
    assert result < pct, 'Observed value of {:.6f} exceeds set threshold of {:.6f}'.format(result, pct)
    print('Observed value: {:.6f}\nThreshold: {:.6f}'.format(result, pct))


def assert_column_unique(**kwargs):
    query = kwargs['templates_dict']['sql']
    print(query)
    assert pd.read_sql(query, get_pandas_connection_string()).iloc[0, 0] == 0


if __name__ == '__main__':
    pass
