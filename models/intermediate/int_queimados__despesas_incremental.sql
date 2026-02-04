{{
  config(
    materialized='incremental',
    unique_key='codigo_interno',
    schema='intermediate_queimados',
    alias='despesas_pagas_incremental',
    on_schema_change='sync_all_columns',

    partition_by={
      "field": "data_despesa",
      "data_type": "date"
    },

    cluster_by=["orgao", "exercicio"]
  )
}}

select *
from {{ ref('stg_queimados__despesas_pagas') }}
where codigo_interno is not null

{% if is_incremental() %}
  and codigo_interno not in (select codigo_interno from {{ this }})
{% endif %}
