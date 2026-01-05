import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

URL = "https://transparencia.queimados.rj.gov.br/sincronia/apidados.rule?sys=LAI"

SERVICE_ACCOUNT_FILE = r"C:\Users\chris\.dbt\monitorpublico.json"

PROJECT_ID = "monitorpublico"
DATASET_ID = "despesas_queimados"
TABLE_ID = "raw_despesas_todas"  # tabela bruta


def fetch_despesas_todas(ano: int) -> pd.DataFrame:
    payload = {
        "api": "despesas_todas",
        "ano": ano
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    print(f"🔎 Consultando despesas_todas para {ano}...")

    response = requests.post(URL, data=json.dumps(payload), headers=headers)
    response.raise_for_status()
    data = response.json()

    status = data.get("status", "").lower()
    if status not in ("sucess", "success"):
        print("⚠️ Erro retornado pela API:", data.get("retorno", data))
        return pd.DataFrame()

    registros = data.get("dados", [])
    if not registros:
        print(f"⚠️ Nenhum registro retornado para {ano}.")
        return pd.DataFrame()

    df = pd.DataFrame(registros)
    df["ano_api"] = ano
    return df


def load_to_bigquery(df: pd.DataFrame):
    if df.empty:
        print("⚠️ DataFrame vazio, nada para carregar.")
        return

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE
    )

    client = bigquery.Client(
        credentials=credentials,
        project=PROJECT_ID
    )

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )

    print(f"⬆️ Carregando {len(df)} linhas para {table_ref}...")
    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()

    print("✅ Carga concluída!")


def main():
    anos = [2025, 2026]
    dfs = []

    for ano in anos:
        df_ano = fetch_despesas_todas(ano)
        if not df_ano.empty:
            dfs.append(df_ano)

    if not dfs:
        print("⚠️ Nenhum dado retornado pela API.")
        return

    df_final = pd.concat(dfs, ignore_index=True)
    load_to_bigquery(df_final)


if __name__ == "__main__":
    main()
