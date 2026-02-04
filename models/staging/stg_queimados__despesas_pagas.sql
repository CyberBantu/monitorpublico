{{ config(
    schema = "staging_queimados",
    alias  = "despesas_pagas",
    materialized = "table"
) }}

select
    -- Identificadores
    cast(codigo_interno as int64) as codigo_interno,
    EMP_PROCESSO_COMPLETO as emp_processo_completo,
    cast(empenho as string) as empenho,
    cast(OP as string) as op,
    cast(NR_OP as string) as nr_op,
    cast(Codigo_Liquidacao as string) as codigo_liquidacao,
    cast(Despesa as string) as despesa,

    -- Dados do favorecido
    CPF_CNPJ_FORMATADA as cpf_cnpj,
    descricao_favorecido,

    -- Classificação orçamentária
    cast(dotacao as string) as dotacao,
    cast(unidade_orcamentaria as string) as unidade_orcamentaria,
    cast(natureza_despeza as string) as natureza_despesa,
    cast(fonte as string) as fonte,
    cast(funcao as string) as funcao,
    cast(subfuncao as string) as subfuncao,
    cast(orgao as string) as orgao,
    cast(secretaria as string) as secretaria,
    cast(acao as string) as acao,
    cast(programa as string) as programa,
    cast(catagoria_economica as string) as categoria_economica,
    cast(catagoria_descricao as string) as categoria_descricao,
    cast(grupo_despesa as string) as grupo_despesa,
    cast(grupo_descricao as string) as grupo_descricao,
    cast(elemento_despesa as string) as elemento_despesa,
    cast(desspesas_descricao as string) as despesa_descricao,

    -- Licitação
    cast(modalidade_licitacao as string) as modalidade_licitacao,
    cast(numero_licitacao as string) as numero_licitacao,

    -- Tipo e descrição
    cast(Tipo as string) as tipo,
    cast(descricao_despesa as string) as descricao_despesa,

    -- Valores monetários (converte string para numeric, tratando vírgula como decimal)
    safe_cast(replace(replace(valor_despesa, '.', ''), ',', '.') as numeric) as valor_despesa,
    safe_cast(replace(replace(valor_despesa_total, '.', ''), ',', '.') as numeric) as valor_despesa_total,
    safe_cast(replace(replace(Valor_Estornado, '.', ''), ',', '.') as numeric) as valor_estornado,
    safe_cast(replace(replace(RETIDO, '.', ''), ',', '.') as numeric) as valor_retido,
    safe_cast(replace(replace(Estorno_do_Pagamento, '.', ''), ',', '.') as numeric) as estorno_do_pagamento,

    -- Datas (converte string 'YYYY-MM-DD HH:MM:SS.S' para DATE)
    safe.parse_date('%Y-%m-%d', substr(cast(data_despesa as string), 1, 10)) as data_despesa,
    safe.parse_date('%Y-%m-%d', substr(cast(data_liquidacao as string), 1, 10)) as data_liquidacao,

    -- Exercício fiscal
    cast(exercicio as int64) as exercicio,
    cast(ano_api as int64) as ano_api,

    -- URL do documento
    URL as url

from {{ source('despesas_queimados', 'raw_despesas_pagas') }}
