# Custom Airflow Operators

A collection of custom operators which are helpful for
building data transformation pipelines in [Apache Airflow](https://airflow.apache.org/).

## Operators

### RedshiftTableConstraintOperator

This operator performs boilerplate data quality checks against a specified table in a Redshift database.
It can be placed at the end of your DAG to verify the integrity of your output,
at the start to verify assumptions on upstream data sources before starting,
or in between data transformation steps to make debugging easier.


##### How to use it

Copy the package `RedshiftTableConstraintOperator` to somewhere you can access
from your dag definition `.py` file.

```python

from .RedshiftTableConstraintOperator import RedshiftTableConstraintOperator

example_task = RedshiftTableConstraintOperator(
    task_id='example_task',
    schema='superb_schema',
    table='terrific_table',
    no_nulls=True,
    unique_rows=True,
    unique_subsets=['session_id'],
    provide_context=True)
```

The argument `no_nulls` can take either a boolean or a list of fields.


### SQLTemplatedPythonOperator

This operator runs an arbitrary python function with a templated SQL file as input.

Useful for implementing bespoke data quality checks using boilerplate functions
 such as `pct_less_than` or `pct_greater_than`. By passing SQL file as template,
 airflow will display it in the _Rendered template_ tab in the web UI,
 which makes it trivial to copy/paste the query for a given dagrun into
 your own IDE to order to debug potential problems.

##### How to use it

 ```python

from SQLTemplatedPythonOperator import SQLTemplatedPythonOperator, assert_pct_less_than

DQ_check = SQLTemplatedPythonOperator(
    task_id='DQ_check',
    python_callable=assert_pct_less_than,
    sql='join_miss_pct.sql',
    op_args=[0.05],
    provide_context=True)
 ```

---
## Tests

### How to run

1. Create a conda environment using `conda env create -f environment.yml`
2. Run `run_tests.sh` file