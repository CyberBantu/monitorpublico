import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


URL = "https://transparencia.queimados.rj.gov.br/sincronia/apidados.rule"

PROJECT_ID = "monitorpublico"
DATASET = "despesas_queimados"
TABLE = "raw_despesas_pagas"

# Anos de 2024 em diante
YEARS = [2024,2025, 2026]


def load_credentials():
    """
    Ordem de prioridade:
    1) Credencial via GitHub Actions (GCP_KEYFILE_JSON)
    2) key.json local
    """
    secret_json = os.getenv("GCP_KEYFILE_JSON")

    # GitHub Actions — credencial no Secret
    if secret_json:
        print("🔐 Usando credenciais do GitHub Secret (GCP_KEYFILE_JSON)")
        info = json.loads(secret_json)
        return service_account.Credentials.from_service_account_info(info)

    # Execução local — arquivo key.json
    if os.path.exists("key.json"):
        print("📁 Usando credenciais locais (key.json)")
        return service_account.Credentials.from_service_account_file("key.json")

    raise RuntimeError(
        "Nenhuma credencial encontrada.\n"
        "Defina GCP_KEYFILE_JSON no ambiente ou crie key.json"
    )


def fetch_from_api(ano: int) -> pd.DataFrame:
    """Busca dados da API usando GET com parâmetros"""
    params = {
        "sys": "LAI",
        "api": "despesas_pagas",
        "ano": ano
    }

    print(f"📡 Consultando API para ano {ano}...")

    response = requests.get(URL, params=params)
    response.raise_for_status()

    raw = response.text
    json_data = json.loads(raw)

    # Extrai os dados do JSON
    if isinstance(json_data, dict):
        if "dados" in json_data:
            dados = json_data["dados"]
        else:
            dados = json_data
    else:
        dados = json_data

    if not dados:
        print(f"⚠️ Nenhum dado retornado para {ano}")
        return pd.DataFrame()

    df = pd.DataFrame(dados)
    df["ano_api"] = ano
    print(f"✓ {len(df)} registros encontrados para {ano}")
    return df


def get_existing_ids(client, id_column: str = "codigo_interno"):
    """Busca IDs já existentes no BigQuery para evitar duplicatas"""
    query = f"""
        SELECT DISTINCT CAST({id_column} AS STRING) AS {id_column}
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    """

    try:
        df = client.query(query).to_dataframe()
        return set(df[id_column])
    except Exception as e:
        print(f"⚠️ Tabela não existe ou erro ao consultar: {e}")
        return set()


def load_to_bq(client, df):
    """Carrega DataFrame no BigQuery"""
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    # Converte todas as colunas para string para evitar conflitos de tipo
    df = df.astype(str)

    # Substitui 'nan' por None para campos vazios
    df = df.replace('nan', None)
    df = df.replace('None', None)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"✅ Dados carregados na tabela {table_ref}")


def main():
    print("🚀 Iniciando ingestão de despesas pagas...")
    print(f"📅 Anos: {YEARS}")

    credentials = load_credentials()

    client = bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID
    )

    # Busca IDs existentes para evitar duplicatas
    existing_ids = get_existing_ids(client)
    print(f"📊 {len(existing_ids)} registros já existentes no BigQuery")

    dfs = []

    for year in YEARS:
        df = fetch_from_api(year)

        if df.empty:
            continue

        # Verifica se tem coluna de ID para deduplicação
        if "codigo_interno" in df.columns:
            df["codigo_interno"] = df["codigo_interno"].astype(str)
            df_new = df[~df["codigo_interno"].isin(existing_ids)]
            print(f"  → {len(df_new)} novos registros para {year}")
            if not df_new.empty:
                dfs.append(df_new)
        else:
            # Se não tem ID, adiciona todos
            dfs.append(df)

    if not dfs:
        print("✔ Nenhum novo registro encontrado")
        return

    final_df = pd.concat(dfs, ignore_index=True)
    print(f"\n📦 Total de novos registros: {len(final_df)}")

    load_to_bq(client, final_df)

    print(f"\n✅ Ingestão concluída! {len(final_df)} registros inseridos.")


if __name__ == "__main__":
    main()
