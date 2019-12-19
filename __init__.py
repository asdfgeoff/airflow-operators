"""
Plugins are defined here, because Airflow will import all modules in `plugins` dir but not packages (folders).
"""

from airflow.plugins_manager import AirflowPlugin
from .RedshiftTableConstraintOperator import RedshiftTableConstraintOperator
from .SQLTemplatedPythonOperator import SQLTemplatedPythonOperator
from .RedshiftJoinCheckOperator import RedshiftJoinCheckOperator


class CustomOperators(AirflowPlugin):
    name = 'custom_operators'
    operators = [RedshiftTableConstraintOperator, SQLTemplatedPythonOperator, RedshiftJoinCheckOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []


if __name__ == '__main__':
    pass
