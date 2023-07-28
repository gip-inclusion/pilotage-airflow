import datetime

import numpy as np
import pandas as pd


# NOTE(vperron): This was already in the code. I can only assume it is the starting data date for NPS ?
START_NPS_DATE = "2022-06-06"
NPS_PROMOTER_THRESHOLD = 9
NPS_DETRACTOR_THRESHOLD = 6


def compute_simplified_nps(df, date, tb_name):
    nps = {}
    promoteurs = df[df["Recommendation"] >= NPS_PROMOTER_THRESHOLD]
    detracteurs = df[df["Recommendation"] <= NPS_DETRACTOR_THRESHOLD]
    nb_promoteurs = len(promoteurs)
    nb_detracteurs = len(detracteurs)
    nb_reco = len(df)

    if len(df) > 0:
        nps_i = (nb_promoteurs - nb_detracteurs) / nb_reco * 100
    else:
        nps_i = np.nan

    nps[date] = nps_i
    df_nps = pd.DataFrame.from_dict(nps, orient="index")
    df_nps = df_nps.reset_index()
    df_nps.rename(columns={"index": "Date", 0: "NPS"}, inplace=True)
    df_nps["tb"] = tb_name
    return df_nps


def model(dbt, session):
    today = datetime.date.today()
    today = today.strftime("%Y-%m-%d")
    week_list = pd.date_range(start=START_NPS_DATE, end=today, freq="W-MON")
    df = dbt.source("oneshot", "suivi_satisfaction")

    tb_pe = [
        "tb 169 - Taux de transformation PE",
        "tb 162 - Fiches de poste en tension PE",
        "tb 149 - Candidatures orientées PE",
        "tb 168 - Délai d'entrée en IAE",
    ]

    tb_siae = [
        "tb 165 - Recrutement SIAE",
    ]

    tb_ddets = [
        "tb 117 - Données IAE DREETS/DDETS",
        "tb 160 - Facilitation de l'embauche DREETS/DDETS",
        "tb 265 - Suivi du contrôle à postériori",
        "tb 267 - Focus auto-prescription DREETS/DDETS",
        "tb 267 - Suivi de l'auto prescription",
    ]

    df_nps = []

    for week_start in week_list:
        # NPS de tous les TBs
        df_cur_week = df[df["Date"] <= week_start]
        df_nps.append(compute_simplified_nps(df_cur_week, week_start, "Tous les TBs"))
        # NPS TB Pôle emploi
        df_cur_week_pe = df_cur_week[df_cur_week["Nom Du Tb"].isin(tb_pe)]
        df_nps.append(compute_simplified_nps(df_cur_week_pe, week_start, "TBs PE"))
        # NPS TB SIAE
        df_cur_week_siae = df_cur_week[df_cur_week["Nom Du Tb"].isin(tb_siae)]
        df_nps.append(compute_simplified_nps(df_cur_week_siae, week_start, "TBs SIAE"))
        # NPS TB DDETS
        df_cur_week_ddets = df_cur_week[df_cur_week["Nom Du Tb"].isin(tb_ddets)]
        df_nps.append(compute_simplified_nps(df_cur_week_ddets, week_start, "TBs DDETS"))

    df = pd.concat(df_nps)
    return df
