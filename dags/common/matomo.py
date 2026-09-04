import calendar
import datetime as dt
import logging
from collections import defaultdict

import requests
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential


logger = logging.getLogger(__name__)

RETRYABLE_HTTP_STATUS_CODES = {500, 502, 503, 504}

PROJECTS_SITE_ID = {
    "carnet de bord": 209,
    "emplois": 117,  # ITOU
    "dora": 211,
    "immersion facile": 207,
    "inclusion": 212,
    "communauté": 206,
    "marché": 136,
    "pilotage": 146,
}


def _raise_for_matomo_error(data):
    if isinstance(data, dict) and data.get("result") == "error":
        raise RuntimeError(f"Matomo API error: {data.get('message')}")


def _redact_token(message, token):
    return message.replace(token, "[TOKEN]") if token else message


def _is_retryable_matomo_request_error(error):
    if isinstance(error, requests.HTTPError):
        return error.response is not None and error.response.status_code in RETRYABLE_HTTP_STATUS_CODES
    return isinstance(error, requests.ConnectionError | requests.Timeout)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=5, min=5, max=10),
    retry=retry_if_exception(_is_retryable_matomo_request_error),
    reraise=True,
)
def _send_matomo_request(matomo_base_url, request_params):
    response = requests.get(
        matomo_base_url,
        headers={"Accept": "application/json"},
        params=request_params,
        timeout=60,
    )
    response.raise_for_status()
    return response


def _request_matomo_api(matomo_base_url, token, method, **params):
    request_params = {
        "module": "API",
        "method": method,
        "format": "json",
        "token_auth": token,
        **params,
    }

    try:
        response = _send_matomo_request(matomo_base_url, request_params)
    except requests.HTTPError as e:
        logger.error("HTTP error: %s", _redact_token(str(e), token))
        raise
    except (requests.ConnectionError, requests.Timeout) as e:
        logger.error("Request error: %s", _redact_token(str(e), token))
        raise

    data = response.json()
    _raise_for_matomo_error(data)
    return data


def _to_int(value):
    if value in (None, ""):
        return 0
    return int(float(value))


def _format_custom_dimension_visit(row):
    return {"department_label": row.get("label") or "", "nb_visits": _to_int(row.get("nb_visits"))}


def _get_custom_dimension_visits(matomo_base_url, token, site_id, segment, period, matomo_date, dimension_id):
    data = _request_matomo_api(
        matomo_base_url,
        token,
        "CustomDimensions.getCustomDimension",
        idSite=site_id,
        period=period,
        date=matomo_date,
        segment=segment,
        idDimension=dimension_id,
        filter_limit=-1,
    )
    if isinstance(data, dict):
        data = data.get("subtable") or data.get("reportData") or []

    return [_format_custom_dimension_visit(row) for row in data]


def get_monthly_visits(matomo_base_url, token, site_id, segment, month):
    data = _request_matomo_api(
        matomo_base_url,
        token,
        "VisitsSummary.get",
        idSite=site_id,
        period="month",
        date=month.strftime("%Y-%m-%d"),
        segment=segment,
    )
    return _to_int(data.get("nb_visits"))


def get_monthly_custom_dimension_visits(matomo_base_url, token, site_id, segment, month, dimension_id):
    visits_by_department_label = defaultdict(int)
    _, last_day = calendar.monthrange(month.year, month.month)

    # Monthly custom-dimension reports can timeout on Matomo, so fetch daily reports and aggregate locally.
    for day in range(1, last_day + 1):
        matomo_date = dt.date(month.year, month.month, day).strftime("%Y-%m-%d")
        for row in _get_custom_dimension_visits(
            matomo_base_url,
            token,
            site_id,
            segment,
            period="day",
            matomo_date=matomo_date,
            dimension_id=dimension_id,
        ):
            visits_by_department_label[row["department_label"]] += row["nb_visits"]

    return [
        {"department_label": department_label, "nb_visits": nb_visits}
        for department_label, nb_visits in sorted(visits_by_department_label.items())
    ]


def get_visits_per_campaign_from_matomo(matomo_base_url, token):
    """
    creates a dataframe composed of all visits for all c0 campaigns and all gip products
    """
    import pandas as pd

    dtf = pd.DataFrame()

    for project_name, site_id in PROJECTS_SITE_ID.items():
        url = (
            f"{matomo_base_url}"
            "?module=API"
            "&method=Live.getLastVisitsDetails"
            "&apiModule=Referrers"
            # we recover all campaigns that are launched by c0
            "&segment=referrerType==campaign;referrerName==c0"
            f"&idSite={site_id}"
            "&expanded=1"
            "&period=month"
            "&format=json"
            f"&token_auth={token}"
        )

        response = requests.get(url, headers={"Accept": "application/json"})
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            logger.error("HTTP error: %s", str(e).replace(f"&token_auth={token}", "&token_auth=[TOKEN]"))
            continue

        data = response.json()
        if isinstance(data, dict):
            logger.error("Matomo %s: %s", data.get("result"), data.get("message"))
            continue

        for result in data:
            dtf = dtf._append(
                {
                    "produit": project_name,
                    "poste": result["referrerKeyword"],
                    "date": result["serverDate"],
                    "visiteur": result["visitorId"],
                    "nb_actions": len(result["actionDetails"]),
                    "duree": round(int(result["visitDuration"]) / 60),
                },
                ignore_index=True,
            )
    return dtf
