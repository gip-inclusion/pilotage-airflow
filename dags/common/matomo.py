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


def get_visits_per_campaign_from_matomo(matomo_base_url, tok):
    """
    creates a dataframe composed of all visits for all c0 campaigns and all gip products
    """
    import pandas as pd

    headers = {"Accept": "application/json"}

    dtf = pd.DataFrame()

    for produit, idsite in produits.items():
        url = (
            f"{matomo_base_url}"
            "?module=API"
            "&method=Live.getLastVisitsDetails"
            "&apiModule=Referrers"
            # we recover all campaigns that are launched by c0
            "&segment=referrerType==campaign;referrerName==c0"
            f"&idSite={idsite}"
            "&expanded=1"
            "&period=month"
            "&format=json"
            f"&token_auth={tok}"
        )

        rep = requests.get(url, headers=headers)

        for json in rep.json():
            infos = {
                "produit": produit,
                "poste": json["referrerKeyword"],
                "date": json["serverDate"],
                "visiteur": json["visitorId"],
                "nb_actions": len(json["actionDetails"]),
                "duree": round(int(json["visitDuration"]) / 60),
            }
            dtf = dtf._append(infos, ignore_index=True)
    return dtf
