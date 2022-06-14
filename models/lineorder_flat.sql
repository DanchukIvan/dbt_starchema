{{-
    config (
      engine='MergeTree()',
      order_by=['LO_ORDERDATE', 'LO_ORDERKEY'],
      partition_by='toYear(LO_ORDERDATE)',
      materialized='incremental',
      inserts_only=True
    )
-}}

{{-init_s3_sources()-}}

  SELECT
      {{ dbt_utils.surrogate_key(['LO_CUSTKEY', 'C_CUSTKEY', 'S_SUPPKEY']) }} As Surrogate_key
      ,{{ dbt_utils.star(source('homework_db','lineorder')) }}
      ,{{ dbt_utils.star(source('homework_db','customer')) }}
      ,{{ dbt_utils.star(source('homework_db','supplier')) }}
      ,{{ dbt_utils.star(source('homework_db','part')) }}
  FROM {{ source('homework_db','lineorder') }} AS l
  INNER JOIN {{ source('homework_db','customer') }} AS c ON c.C_CUSTKEY = l.LO_CUSTKEY
  INNER JOIN {{ source('homework_db','supplier') }} AS s ON s.S_SUPPKEY = l.LO_SUPPKEY
  INNER JOIN {{ source('homework_db','part') }} AS p ON p.P_PARTKEY = l.LO_PARTKEY
  {% if is_incremental() %}
    WHERE {{ dbt_utils.surrogate_key(['LO_CUSTKEY', 'C_CUSTKEY', 'S_SUPPKEY']) }} >= (SELECT MAX({{ dbt_utils.surrogate_key(['LO_CUSTKEY', 'C_CUSTKEY', 'S_SUPPKEY']) }}) FROM {{this}})
  {% endif %}