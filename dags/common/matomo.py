import logging

import requests


logger = logging.getLogger(__name__)

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
