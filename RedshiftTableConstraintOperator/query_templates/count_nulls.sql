SELECT COUNT(*)
FROM {{ schema }}.{{ table }}
WHERE {{ column }} IS NULL
{% if date_col is defined and date_col is not none -%}
  AND {{date_col}} >= '{{ date_from }}'
  AND {{date_col}} < '{{ date_until }}'
{%- endif %}