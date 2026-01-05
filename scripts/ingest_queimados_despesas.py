import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account


URL = "https://transparencia.queimados.rj.gov.br/sincronia/apidados.rule?sys=LAI"

PROJECT_ID = "monitorpublico"
DATASET = "despesas_queimados"
TABLE = "raw_despesas_todas"

YEARS = [2025, 2026]


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
    payload = {
        "api": "despesas_todas",
        "ano": ano
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    print(f"📡 Consultando API para ano {ano}...")

    res = requests.post(URL, json=payload, headers=headers)
    res.raise_for_status()

    data = res.json()

    if data.get("status", "").lower() not in ("sucess", "success"):
        print(f"⚠️ API retornou erro para {ano}: {data}")
        return pd.DataFrame()

    df = pd.DataFrame(data.get("dados", []))
    df["ano_api"] = ano
    return df


def get_existing_ids(client):
    query = f"""
        SELECT DISTINCT CAST(codigo_interno AS STRING) AS codigo_interno
        FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
    """

    try:
        df = client.query(query).to_dataframe()
        return set(df["codigo_interno"])
    except Exception:
        return set()


def load_to_bq(client, df):
    table_ref = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )

    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()


def main():
    print("🚀 Iniciando ingestão incremental...")

    credentials = load_credentials()

    client = bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID
    )

    existing_ids = get_existing_ids(client)

    dfs = []

    for year in YEARS:
        df = fetch_from_api(year)

        if df.empty:
            continue

        df["codigo_interno"] = df["codigo_interno"].astype(str)

        # Remove registros já existentes
        df = df[~df["codigo_interno"].isin(existing_ids)]

        if not df.empty:
            dfs.append(df)

    if not dfs:
        print("✔ Nenhum novo registro encontrado")
        return

    final_df = pd.concat(dfs, ignore_index=True)

    load_to_bq(client, final_df)

    print(f"✅ Inseridos {len(final_df)} novos registros.")


if __name__ == "__main__":
    main()
