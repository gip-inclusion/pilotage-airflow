import pandas as pd
import requests

produits = {
    "carnet de bord": 209,
    "emplois": 117,  # ITOU
    "dora": 211,
    "immersion facile": 207,
    "inclusion": 212,
    "communauté": 206,
    "marché": 136,
    "pilotage": 146,
}


def get_visit_info(produit, json_visit):
    """
    from a json visit extracted from matomo via api, returns a dict of relevant informations
    """
    infos = {
        "produit": produit,
        "poste": json_visit["referrerKeyword"],
        "date": json_visit["serverDate"],
        "visiteur": json_visit["visitorId"],
        "nb_actions": len(json_visit["actionDetails"]),
        "duree": json_visit["visitDurationPretty"],
    }
    return infos


def get_visits_per_campaign(tok):
    """
    creates a dataframe composed of all visits for all c0 campaigns and all gip products
    """
    headers = {"Accept": "application/json"}

    dtf = pd.DataFrame()

    for produit, idsite in produits.items():
        url = (
            "https://matomo.inclusion.beta.gouv.fr/index.php"
            "?module=API"
            "&method=Live.getLastVisitsDetails"
            "&apiModule=Referrers"
            "&segment=referrerType==campaign;referrerName==c0"
            "&idSite={}"
            "&expanded=1"
            "&period=month"
            "&date=2023-05-01"
            "&format=json"
            "&token_auth={}".format(idsite, tok)
        )

        rep = requests.get(url, headers=headers)
        for json in rep.json():
            infos = get_visit_info(produit, json)
            dtf = dtf._append(infos, ignore_index=True)
    return dtf
