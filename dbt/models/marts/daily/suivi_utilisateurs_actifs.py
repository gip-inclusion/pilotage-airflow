import datetime

import pandas as pd


def get_month_from_date(date):
    return datetime.date(date.year, date.month, 1)


def compare_months(df):
    # recover months pairs
    df["month"] = df["semaine"].apply(get_month_from_date)
    months = list(set([datetime.date(x.year, x.month, 1) for x in df["semaine"]]))
    month_pairs = [(months[i], months[j]) for i in range(len(months)) for j in range(i + 1, len(months))]

    tbs = list(set(df["nom_tb"]))

    outdtf = pd.DataFrame()

    tb_col = []
    ref_col = []
    comp_col = []
    visiteurs_actifs = []
    nb_visiteurs_actifs = []

    for ref_month, comp_month in month_pairs:
        for tb in tbs:
            tb_df = df[df["nom_tb"] == tb]

            tb_col.append(tb)
            ref_col.append(ref_month)
            comp_col.append(comp_month)

            # extract users that came in tb in ref_month
            ref_users = list(tb_df[tb_df["month"] == ref_month]["liste_utilisateurs"])
            ref_users = list(set([usr for sublist in ref_users for usr in sublist]))

            # extract users that came in tb in comp_month
            comp_users = list(tb_df[tb_df["month"] == comp_month]["liste_utilisateurs"])
            comp_users = list(set([usr for sublist in comp_users for usr in sublist]))

            # get difference
            diff = [usr for usr in comp_users if usr in ref_users]
            visiteurs_actifs.append(diff)
            nb_visiteurs_actifs.append(len(diff))

    outdtf["tb"] = tb_col
    outdtf["ref"] = ref_col
    outdtf["comp"] = comp_col
    outdtf["visiteurs_actifs"] = visiteurs_actifs
    outdtf["nb_visiteurs_actifs"] = nb_visiteurs_actifs

    return outdtf


def compare_quarters(df):
    tbs = list(set(df["nom_tb"]))

    # recover quarters pairs
    years = [datetime.datetime.today().year, datetime.datetime.today().year - 1]
    quarters_month = [[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12]]
    quarters_names = [t + " - " + str(y) for t in ["T1", "T2", "T3", "T4"] for y in years]
    quarters = [[datetime.date(y, m, 1) for m in q] for q in quarters_month for y in years]

    quarters_pairs = [
        (quarters[i], quarters[j], quarters_names[i], quarters_names[j])
        for i in range(len(quarters))
        for j in range(i + 1, len(quarters))
    ]

    outdtf = pd.DataFrame()

    tb_col = []
    ref_col = []
    comp_col = []
    visiteurs_actifs = []
    nb_visiteurs_actifs = []

    for ref_quarter, comp_quarter, ref_quarter_name, comp_quarter_name in quarters_pairs:
        for tb in tbs:
            tb_df = df[df["nom_tb"] == tb]

            tb_col.append(tb)
            ref_col.append(ref_quarter_name)
            comp_col.append(comp_quarter_name)

            # extract users that came in tb in ref_month
            ref_mask = (
                (tb_df["month"] == ref_quarter[0])
                | (tb_df["month"] == ref_quarter[1])
                | (tb_df["month"] == ref_quarter[2])
            )
            ref_users = list(tb_df[ref_mask]["liste_utilisateurs"])
            ref_users = list(set([usr for sublist in ref_users for usr in sublist]))

            # extract users that came in tb in comp_month
            comp_mask = (
                (tb_df["month"] == comp_quarter[0])
                | (tb_df["month"] == comp_quarter[1])
                | (tb_df["month"] == comp_quarter[2])
            )
            comp_users = list(tb_df[comp_mask]["liste_utilisateurs"])
            comp_users = list(set([usr for sublist in comp_users for usr in sublist]))

            # get difference
            diff = [usr for usr in comp_users if usr in ref_users]
            visiteurs_actifs.append(diff)
            nb_visiteurs_actifs.append(len(diff))

    outdtf["tb"] = tb_col
    outdtf["ref"] = ref_col
    outdtf["comp"] = comp_col
    outdtf["visiteurs_actifs"] = visiteurs_actifs
    outdtf["nb_visiteurs_actifs"] = nb_visiteurs_actifs

    return outdtf


def compare_periods(df):
    df["semaine"] = pd.to_datetime(df["semaine"], utc=True).dt.date

    month_df = compare_months(df)
    quarter_df = compare_quarters(df)

    out_df = pd.concat([month_df, quarter_df], ignore_index=True)

    return out_df


def model(dbt, session):
    df = dbt.ref("suivi_visites_tb_prive_semaine")
    outdtf = compare_periods(df)
    return outdtf