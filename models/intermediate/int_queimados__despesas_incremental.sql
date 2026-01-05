{{
  config(
    materialized = 'incremental',
    unique_key = 'id_registro_api',
    on_schema_change = 'sync_all_columns',

    partition_by = {
      "field": "data_despesa",
      "data_type": "timestamp"
    },

    cluster_by = ["orgao", "ano_exercicio"]
  )
}}

with base as (
    select *
    from {{ ref('stg_queimados__despesas_todas') }}
)

select
    *
from base

{% if is_incremental() %}

    -- Só insere registros novos
    where id_registro_api not in (
        select id_registro_api
        from {{ this }}
    )

{% endif %}
