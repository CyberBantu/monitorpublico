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


def fetch_from_api(ano):
    payload = {
        "api": "despesas_todas",
        "ano": ano
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    res = requests.post(URL, json=payload, headers=headers)
    res.raise_for_status()

    data = res.json()

    if data.get("status", "").lower() not in ("sucess", "success"):
        print(f"[WARN] API retornou erro para ano {ano}: {data}")
        return pd.DataFrame()

    df = pd.DataFrame(data.get("dados", []))
    df["ano_api"] = ano
    return df


def get_existing_ids(client):
    query = f"""
        select distinct cast(codigo_interno as string) as codigo_interno
        from `{PROJECT_ID}.{DATASET}.{TABLE}`
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
    print("Iniciando ingestão...")

    credentials = service_account.Credentials.from_service_account_file("key.json")

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

        df = df[~df["codigo_interno"].astype(str).isin(existing_ids)]

        if not df.empty:
            dfs.append(df)

    if not dfs:
        print("Nenhum novo registro encontrado.")
        return

    final_df = pd.concat(dfs, ignore_index=True)

    load_to_bq(client, final_df)

    print(f"Inseridos {len(final_df)} novos registros.")


if __name__ == "__main__":
    main()
