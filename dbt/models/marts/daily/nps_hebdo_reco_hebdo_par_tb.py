import datetime

import numpy as np
import pandas as pd


START_NPS_DATE = "2022-06-06"
NPS_PROMOTER_THRESHOLD = 9
NPS_DETRACTOR_THRESHOLD = 6


def get_nps(reco_dtf):
    # recover prom and det list of reco before given date
    promoteurs = reco_dtf[reco_dtf["Recommendation"] >= NPS_PROMOTER_THRESHOLD]
    detracteurs = reco_dtf[reco_dtf["Recommendation"] <= NPS_DETRACTOR_THRESHOLD]
    nb_promoteurs = len(promoteurs)
    nb_detracteurs = len(detracteurs)
    nb_reco = len(reco_dtf)

    if len(reco_dtf) > 0:
        nps_i = (nb_promoteurs / nb_reco - nb_detracteurs / nb_reco) * 100
    else:
        nps_i = np.nan

    return nps_i


def model(dbt, session):
    today = datetime.date.today()
    today = today.strftime("%Y-%m-%d")
    week_list = pd.date_range(start=START_NPS_DATE, end=today, freq="W-MON")

    df_nps = pd.DataFrame()
    df_nps["date"] = 0
    df_nps["tb"] = 0
    df_nps["nps"] = 0

    df = dbt.source("oneshot", "suivi_satisfaction")
    df = df[["Recommendation", "Nom Du Tb", "Date"]]
    df["Date"] = pd.to_datetime(df["Date"])

    tbs = list(set(df["Nom Du Tb"]))

    # loop over dates
    for i in week_list:
        # recover data for current date
        df_cur_week = df[df["Date"] <= i]

        # get global nps
        glob_nps = get_nps(df_cur_week)
        glob_nps_data = {"date": i, "tb": "tous tb", "nps": glob_nps, "users": len(df_cur_week)}
        df_nps = df_nps.append(glob_nps_data, ignore_index=True)

        # get PE nps
        tb_pe = [
            "tb 169 - Taux de transformation PE",
            "tb 162 - Fiches de poste en tension PE",
            "tb 149 - Candidatures orientées PE",
            "tb 168 - Délai d'entrée en IAE",
        ]
        df_cur_week_pe = df_cur_week[df_cur_week["Nom Du Tb"].isin(tb_pe)]
        pe_nps = get_nps(df_cur_week_pe)
        glob_nps_data = {"date": i, "tb": "tb PE", "nps": pe_nps, "users": len(df_cur_week_pe)}
        df_nps = df_nps.append(glob_nps_data, ignore_index=True)

        # get specific nps
        for tb in tbs:
            # recover recommandations for given tb
            df_cur_tb = df_cur_week[df_cur_week["Nom Du Tb"] == tb]

            nb_reco = 0
            nb_promoteurs = 0
            nb_detracteurs = 0

            df_cur_tb_cur_date = df_cur_tb[df_cur_tb["Date"] <= i]

            # nb reco for cur week is nb_reco already treated + nb reco of current week
            nb_reco = len(df_cur_tb_cur_date)

            # recover prom and det list of reco before given date
            promoteurs = df_cur_tb_cur_date[df_cur_tb_cur_date["Recommendation"] >= 9]
            detracteurs = df_cur_tb_cur_date[df_cur_tb_cur_date["Recommendation"] <= 6]
            nb_promoteurs = len(promoteurs)
            nb_detracteurs = len(detracteurs)

            if len(df_cur_tb_cur_date) > 0:
                nps_i = (nb_promoteurs / nb_reco - nb_detracteurs / nb_reco) * 100
            else:
                nps_i = np.nan

            data = {"date": i, "tb": tb, "nps": nps_i, "users": len(df_cur_tb_cur_date)}

            df_nps = df_nps.append(data, ignore_index=True)

    df_nps["date"] = pd.to_datetime(df_nps["date"])
    df_nps["nps"] = df_nps["nps"].astype(float)
    df_nps["users"] = df_nps["users"].astype(float)

    return df_nps
