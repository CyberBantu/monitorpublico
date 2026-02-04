{{
  config(
    materialized = "table",
    schema = "marts_queimados",
    alias = "gastos_por_secretaria"
  )
}}

with base as (
    select
        -- Dimensões para filtros no Looker
        exercicio,
        data_despesa,

        -- Hierarquia organizacional
        secretaria,
        orgao,
        unidade_orcamentaria,

        -- Classificação orçamentária
        funcao,
        subfuncao,
        programa,
        acao,
        categoria_economica,
        categoria_descricao,
        grupo_despesa,
        grupo_descricao,
        elemento_despesa,
        despesa_descricao,
        natureza_despesa,
        fonte,

        -- Licitação
        modalidade_licitacao,

        -- Tipo de despesa
        tipo,

        -- Valores
        valor_despesa,
        valor_despesa_total,
        valor_estornado,
        valor_retido,
        estorno_do_pagamento

    from {{ ref('int_queimados__despesas_incremental') }}
)

select
    -- Dimensões de tempo
    exercicio as ano_exercicio,
    data_despesa,

    -- Hierarquia organizacional
    coalesce(secretaria, orgao) as secretaria_padronizada,
    orgao,
    unidade_orcamentaria,

    -- Classificação orçamentária (para filtros)
    funcao,
    subfuncao,
    programa,
    acao,
    categoria_economica,
    categoria_descricao,
    grupo_despesa,
    grupo_descricao,
    elemento_despesa,
    despesa_descricao,
    natureza_despesa,
    fonte,

    -- Licitação
    modalidade_licitacao,

    -- Tipo
    tipo,

    -- Métricas agregadas
    count(*) as total_registros,
    sum(valor_despesa) as total_despesa,
    sum(valor_despesa_total) as total_despesa_acumulado,
    sum(valor_estornado) as total_estornado,
    sum(valor_retido) as total_retido,
    sum(estorno_do_pagamento) as total_estorno_pagamento,

    -- Valor líquido (despesa - estornos)
    sum(valor_despesa) - coalesce(sum(valor_estornado), 0) - coalesce(sum(estorno_do_pagamento), 0) as valor_liquido

from base
group by
    exercicio,
    data_despesa,
    secretaria,
    orgao,
    unidade_orcamentaria,
    funcao,
    subfuncao,
    programa,
    acao,
    categoria_economica,
    categoria_descricao,
    grupo_despesa,
    grupo_descricao,
    elemento_despesa,
    despesa_descricao,
    natureza_despesa,
    fonte,
    modalidade_licitacao,
    tipo

