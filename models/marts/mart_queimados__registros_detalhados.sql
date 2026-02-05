{{
  config(
    materialized = "table",
    schema = "marts_queimados",
    alias = "registros_detalhados"
  )
}}

-- Registros individuais de despesas pagas (não agregados)
-- Para visualização granular no dashboard

select
    -- Identificador único
    codigo_interno,

    -- Datas
    data_despesa,
    exercicio as ano,

    -- Hierarquia organizacional
    coalesce(secretaria, orgao) as secretaria,
    orgao,
    unidade_orcamentaria,

    -- Classificação orçamentária
    funcao,
    subfuncao,
    programa,
    acao,
    elemento_despesa,
    despesa_descricao,
    natureza_despesa,
    fonte,
    categoria_economica,
    categoria_descricao,
    grupo_despesa,
    grupo_descricao,

    -- Licitação
    modalidade_licitacao,
    numero_licitacao,

    -- Favorecido
    descricao_favorecido as favorecido,
    cpf_cnpj,
    tipo,

    -- Valores
    valor_despesa,
    valor_estornado,
    valor_retido,

    -- Processo
    empenho,
    op,
    emp_processo_completo as processo

from {{ ref('int_queimados__despesas_incremental') }}
where valor_despesa is not null
order by data_despesa desc, valor_despesa desc
