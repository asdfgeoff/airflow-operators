WITH duplicates AS (
  SELECT
    {% set comma = joiner(", ") -%}
    {%- for column in cols -%}
    {{ comma() }}{{ column }}
    {%- endfor -%}
    , COUNT(*) AS num_rows
    {%- if date_col is defined and date_col is not none -%}
    , BOOL_OR(CASE WHEN {{ date_col }} >= {{ date_from }} AND {{ date_col }} < {{ date_until }} THEN TRUE ELSE FALSE END) AS is_in_date_range
    {%- endif %}
  FROM {{ schema }}.{{ table }}
  WHERE
    {%- set and = joiner("AND ") %}
    {%- for column in cols %}
    {{ and() }}{{ column }} IS NOT NULL
    {%- endfor %}
  GROUP BY
    {% set comma = joiner(", ") -%}
    {%- for column in cols -%}
    {{ comma() }}{{ column }}
    {%- endfor %}
  HAVING COUNT(*) > 1
  )
SELECT NVL(SUM(num_rows), 0) AS num_duplicates
FROM duplicates
{%- if date_col is defined and date_col is not none -%}
WHERE is_in_date_range IS TRUE
{%- endif %}
;