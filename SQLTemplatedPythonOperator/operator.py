from airflow.operators.python_operator import PythonOperator
from typing import Optional


class SQLTemplatedPythonOperator(PythonOperator):
    """ Extend PythonOperator to receive a templated SQL query and also to display it in the "Rendered Template" tab in Airflow's UI.

    This is very helpful for troubleshooting specific task instances, since you can copy a propertly formatted query directly from
    the web UI rather than copying the contents of "templates_dict" and parsing it manually.

    Args:
        sql (str): File path or query text containing jinja2 variables to be filled by airflow templating engine.
        python_callable (func): Access final sql text from inside using kwargs['templates_dict']['query']

    """
    template_ext = ('.sql',)
    template_fields = ('sql', 'templates_dict')
    ui_color = "#ffe5cc"
    ui_fgcolor = "#000"

    def __init__(self,
                 sql: str,
                 op_args: Optional[list] = None,
                 op_kwargs: Optional[list] = None,
                 *args, **kwargs) -> None:

        super(SQLTemplatedPythonOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.templates_dict = {'sql': sql}


if __name__ == '__main__':
    pass
