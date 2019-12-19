SELECT AVG(CASE WHEN {{ params.table_A }}.{{ params.joinkey_A }} IS NULL THEN 0.00 ELSE 1.00 END) AS pct_hit
FROM {{ params.schema_A }}.{{params.table_A}} a
LEFT JOIN {{ params.schema_B }}.{{ params.table_B }} b
  ON a.{{ params.joincol_A }} = b.{{ params.joincol_B }}
  AND b.{{ params.sortkey_B }} >= '{{ ds }}'
  AND b.{{ params.sortkey_B }} < '{{ macros.ds_add(ds, 1) }}'
WHERE a.{{ params.sortkey_A }} >= '{{ ds }}'
  AND a.{{ params.sortkey_A }} < '{{ macros.ds_add(ds, 1) }}'
;