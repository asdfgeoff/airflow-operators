import os
import psycopg2
from typing import Union, List
from jinja2 import Template
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow import AirflowException

LOCAL_DIR = os.path.dirname(os.path.abspath(__file__))

REDSHIFT_CONNECTION_NAME = ''  # Change this to reflect your own connection name


def get_psycopg2_creds():
    etl_conn = BaseHook.get_connection(REDSHIFT_CONNECTION_NAME)
    return dict(user=etl_conn.login,
                password=etl_conn.get_password(),
                host=etl_conn.host,
                port=etl_conn.port,
                dbname=etl_conn.schema)


class RedshiftTableConstraintOperator(BaseOperator):
    """ Check uniqueness and null-value constraints against columns in a single table in Amazon Redshift.

    Add upstream of a task to validate your assumptions about incoming data sources,
    or downstream of a task to sanity-check your processing logic and catch potential bugs.

    Args:
        schema (str): File path or query text containing jinja2 variables to be filled by airflow templating engine.
        table (str): Access final sql text from inside using kwargs['templates_dict']['query']
        date_col (str): Name of sortkey column in target table, to be used to constrain date range of quality checks.
        no_nulls (list, bool): A list of columns which should not contain nulls, or a value of True to indicate all columns.
        unique_rows (bool): If True, each row of the table should be unique.
        unique_subset (list, bool): A list of a subset of columns, within which rows should be unique.
    """

    ui_color = "#e6ccff"
    ui_fgcolor = "#000"

    def __init__(self,
                 schema: str,
                 table: str,
                 date_col: str = None,
                 no_nulls: Union[bool, list, None] = False,
                 unique_rows: bool = False,
                 unique_subset: Union[list, bool, None] = False,
                 *args, **kwargs) -> None:

        super(RedshiftTableConstraintOperator, self).__init__(*args, **kwargs)

        self.schema = schema
        self.table = table
        self.date_col = date_col
        self.no_nulls = no_nulls
        self.unique_rows = unique_rows
        self.unique_subset = unique_subset
        self.dt_from = None
        self.dt_until = None

    def execute(self, context: dict) -> None:
        # TODO: separate into summarize method
        print('RUNNING CHECKS ON on {}.{}'.format(self.schema, self.table))
        print('Check for nulls? {}'.format(str(self.no_nulls).upper()))
        print('Check for unique rows? {}'.format(str(self.unique_rows).upper()))
        print('Check for unique subsets? {}'.format(str(self.unique_subset).upper()))

        self.dt_from = context['execution_date'].strftime('%Y-%m-%d')
        self.dt_until = context['next_execution_date'].strftime('%Y-%m-%d')

        conn = psycopg2.connect(**get_psycopg2_creds())  # cannot be stored as class property since not pickle-able

        all_cols = self.discover_columns(self.schema, self.table, conn)

        # TODO: write a decorator to listify string arguments
        if self.no_nulls is True:
            self.assert_no_nulls(all_cols, conn)
        elif isinstance(self.no_nulls, list):
            self.assert_no_nulls(self.no_nulls, conn)
        elif isinstance(self.no_nulls, str):
            self.assert_no_nulls([self.no_nulls], conn)
        elif self.no_nulls is False or self.no_nulls is None:
            pass
        else:
            raise ValueError()

        if self.unique_rows:
            self.assert_unique_values(all_cols, conn)

        if isinstance(self.unique_subset, list):
            self.assert_unique_values(self.unique_subset, conn)
        elif isinstance(self.unique_subset, str):
            self.assert_unique_values([self.unique_subset], conn)
        elif self.unique_subset is False or self.unique_subset is None:
            pass
        else:
            raise ValueError()

        conn.close()

    @staticmethod
    def discover_columns(schema: str, table: str, conn) -> list:
        with open(os.path.join(LOCAL_DIR, 'query_templates', 'discover_columns.sql')) as f:
            query = Template(f.read()).render(schema=schema, table=table)

        with conn.cursor() as cur:
            cur.execute(query)
            raw_output = cur.fetchall()

        if len(raw_output) == 0:
            raise AirflowException('Cound not discover columns for table {}.{}'.format(schema, table))

        return [row[0] for row in raw_output]

    def assert_no_nulls(self, cols: list, conn) -> None:
        with open(os.path.join(LOCAL_DIR, 'query_templates', 'count_nulls.sql')) as f:
            template = Template(f.read())

        problem_cols = []

        for col in cols:
            query = template.render(schema=self.schema,
                                    table=self.table,
                                    column=col,
                                    date_col=self.date_col,
                                    date_from=self.dt_from,
                                    date_until=self.dt_until)

            print(query)

            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchall()[0][0]

            if result > 0:
                problem_cols.append(col)

        if len(problem_cols) == 0:
            print('Success! All columns are non-null: {}'.format(', '.join(cols)))
        else:
            raise AirflowException('Uh-oh! These columns have null value_col: {}'.format(', '.join(problem_cols)))

    def assert_unique_values(self, cols: List[str], conn) -> None:
        with open(os.path.join(LOCAL_DIR, 'query_templates', 'count_duplicates.sql')) as f:
            template = Template(f.read())

        print('ASSERT UNQUE VALUES IN SUBSET: {}'.format(cols))
        query = template.render(schema=self.schema, table=self.table, cols=cols)
        print(query)

        with conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchall()[0][0]

        if result == 0:
            print('Success! All rows are unique across columns: {}'.format(', '.join(cols)))
        else:
            raise AirflowException('Uh-oh! Some rows are duplicated across columns {}'.format(', '.join(cols)))


if __name__ == '__main__':
    pass
