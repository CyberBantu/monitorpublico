{{ config(materialized='table') }}

with base as (

    select *
    from {{ source('queimados', 'raw_despesas_todas') }}

),

transformacao as (

    select
        
        -- CONTROLE / CHAVES
        EMP_PROCESSO_COMPLETO    as processo,
        SAFE_CAST(ano AS INT64)                         as ano_exercicio,
        SAFE_CAST(ano_api AS INT64)                     as ano_coleta,

        -- FAVORECIDO
        descricao_favorecido                            as favorecido,
        CPF_CNPJ_FORMATADA                              as documento_favorecido,
        Tipo                                            as tipo_favorecido,

        -- CLASSIFICAÇÃO ORÇAMENTÁRIA
        unidade_orcamentaria,
        orgao,
        nome_secretria as secretaria,
        funcao,
        subfuncao,
        PROGRAMA                                        as programa,
        acao,
        natureza_despeza                                as natureza_despesa,
        catagoria_economica,
        catagoria_descricao,
        grupo_despesa,
        grupo_descricao,
        elemento_despesa,
        descricao_elemento_despesa,
        dotacao,
        fonte,
        modalidade_licitacao,
        numero_licitacao,

        -- MOVIMENTAÇÃO
        Despesa                                         as tipo_movimentacao,
        Estorno_do_Empenho                              as flag_estorno_empenho,

        empenho                                         as numero_empenho,
        OP                                              as numero_op,

        -- VALORES
        SAFE_CAST(valor_despesa AS NUMERIC)             as valor_empenhado,
        SAFE_CAST(valor_despesa_total AS NUMERIC)       as valor_empenhado_total,
        SAFE_CAST(valor_liquidado AS NUMERIC)           as valor_liquidado,
        SAFE_CAST(valor_pago_liquido AS NUMERIC)        as valor_pago_liquido,
        SAFE_CAST(valor_retido AS NUMERIC)              as valor_retido,
        SAFE_CAST(valor_estorno_pagamento AS NUMERIC)   as valor_estorno_pagamento,
        SAFE_CAST(valor_estorno_liquidacao AS NUMERIC)  as valor_estorno_liquidacao,

        -- DATA
        SAFE_CAST(data_despesa AS TIMESTAMP)            as data_despesa,

        -- TEXTO LIVRE
        Objeto                                          as objeto_despesa

    from base
)

select * from transformacao
