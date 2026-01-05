{{
  config(
    materialized = "table",
    schema = "marts",
    alias = "gastos_por_secretaria"
  )
}}

with base as (

    select
        ano_exercicio,
        secretaria,
        orgao,
        valor_empenhado_total,
        valor_pago_liquido,
        valor_liquidado
    from {{ ref('int_queimados__despesas_incremental') }}

)

select
    ano_exercicio,

    coalesce(secretaria, orgao) as secretaria_padronizada,

    sum(valor_empenhado_total) as total_empenhado,
    sum(valor_liquidado)       as total_liquidado,
    sum(valor_pago_liquido)    as total_pago
from base
group by 1, 2
order by ano_exercicio, secretaria_padronizada
