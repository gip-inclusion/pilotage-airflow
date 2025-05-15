import logging

import httpx
import pandas as pd

from airflow.models import Variable

logger = logging.getLogger(__name__)

from dags.common import dbt, default_dag_args

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

def api_client() -> httpx.Client:
    return httpx.Client(
        base_url=Variable.get("API_IMMERSION_FACILITEE_BASE_URL"),
        headers={"Authorization": "Bearer {}".format(Variable.get("API_IMMERSION_FACILITEE_TOKEN"))},
        timeout=httpx.Timeout(timeout=5, read=30),
    )

def get_all_items(path):
    client = api_client()

    page_to_fetch = 1
    while True:
        response = client.get(path, params={"size": 1000, "page": page_to_fetch})
        response.raise_for_status()
        data = response.json()
        logger.info("Got %r items, metadata=%r", len(data["items"]), {k: v for k, v in data.items() if k != "items"})
        yield from data["items"]
        page_to_fetch = data["page"] + 1
        if page_to_fetch > data["pages"]:
            break