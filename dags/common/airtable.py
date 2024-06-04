import pandas as pd
import requests
from airflow.models import Variable


def connection_airtable(table_name):
    api_token = Variable.get("TOKEN_API_AIRTABLE_MON_RECAP")
    base_id = Variable.get("BASE_ID_AIRTABLE_MON_RECAP")
    url = f"https://api.airtable.com/v0/{base_id}/{table_name}"
    headers = {"Authorization": f"Bearer {api_token}"}
    return url, headers


def fetch_airtable_data(url, headers):
    records = []
    params = {}
    response = requests.get(url=url, params=params, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")
    data = response.json()
    records.extend(data["records"])
    df = pd.DataFrame([record["fields"] for record in records])
    return df
