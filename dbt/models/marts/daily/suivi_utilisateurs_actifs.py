import datetime

import pandas as pd


def get_month_from_date(date):
    return datetime.date(date.year, date.month, 1)


def users_came_again(df, ref_quarter, comp_quarter):
    """
    extract from df users that came in ref_quarter and already came in comp_quarter
    """

    # extract users that came in ref_month
    ref_mask = (df["month"] == ref_quarter[0]) | (df["month"] == ref_quarter[1]) | (df["month"] == ref_quarter[2])
    ref_users = list(df[ref_mask]["liste_organisations"])
    ref_users = list(set([usr for sublist in ref_users for usr in sublist]))

    # extract users that came in comp_month
    comp_mask = (df["month"] == comp_quarter[0]) | (df["month"] == comp_quarter[1]) | (df["month"] == comp_quarter[2])
    comp_users = list(df[comp_mask]["liste_organisations"])
    comp_users = list(set([usr for sublist in comp_users for usr in sublist]))

    # get difference
    diff = [usr for usr in comp_users if usr in ref_users]

    return diff


def compare_months(df):
    # recover months pairs
    months = list(set([datetime.date(x.year, x.month, 1) for x in df["semaine"]]))
    months.sort(reverse=True)
    # add month, month-1 pairs
    month_pairs = [(months[i], months[i + 1]) for i in range(0, len(months) - 1)]
    # add month, month prec year pairs (only if exists)
    month_pairs += [(months[i], months[i + 12]) for i in range(len(months)) if i + 12 < len(months)]

    df_cols = [
        "tb",
        "profil",
        "type",
        "région",
        "département",
        "ref",
        "comp",
        "utilisateurs_revenus",
        "nb_utilisateurs_revenus",
    ]
    outdf = pd.DataFrame(columns=df_cols)

    for ref_month, comp_month in month_pairs:
        for tb, tb_df in df.groupby("nom_tb"):
            for typ, dftype in tb_df.groupby("type_utilisateur"):
                for profil, dfprofil in dftype.groupby("profil"):
                    for region, dfregion in dfprofil.groupby("région"):
                        for dept, dfdept in dfregion.groupby("département_num"):
                            # extract users that came in tb in ref_month
                            ref_users = list(dfdept[dfdept["month"] == ref_month]["liste_organisations"])
                            ref_users = list(set([usr for sublist in ref_users for usr in sublist]))

                            # extract users that came in tb in comp_month
                            comp_users = list(dfdept[dfdept["month"] == comp_month]["liste_organisations"])
                            comp_users = list(set([usr for sublist in comp_users for usr in sublist]))

                            # get difference
                            diff = [usr for usr in comp_users if usr in ref_users]

                            outdf = outdf.append(
                                {
                                    "tb": tb,
                                    "profil": profil,
                                    "type": typ,
                                    "région": region,
                                    "département": dept,
                                    "ref": ref_month,
                                    "comp": comp_month,
                                    "utilisateurs_revenus": diff,
                                    "nb_utilisateurs_revenus": len(diff),
                                },
                                ignore_index=True,
                            )

    return outdf


def compare_quarters(df):
    tbs = list(set(df["nom_tb"]))

    # recover quarters pairs
    years = [datetime.datetime.today().year - i for i in range(0, 3)]
    quarters_month = [[10, 11, 12], [7, 8, 9], [4, 5, 6], [1, 2, 3]]
    quarters_names = [t + " - " + str(y) for y in years for t in ["T4", "T3", "T2", "T1"]]
    quarters = [[datetime.date(y, m, 1) for m in q] for y in years for q in quarters_month]

    # ajout des paires N/N-1
    quarters_pairs = [
        (quarters[i], quarters[i + 1], quarters_names[i], quarters_names[i + 1])
        for i in range(0, len(quarters_names) - 1)
    ]
    # ajout des paires Na/ Na-1 / Na-2
    for i in range(0, 4):
        # Na / Na-1
        quarters_pairs.append((quarters[i], quarters[i + 4], quarters_names[i], quarters_names[i + 4]))
        # Na / Na-2
        quarters_pairs.append((quarters[i], quarters[i + 8], quarters_names[i], quarters_names[i + 8]))
        # Na-1 / Na -2
        quarters_pairs.append((quarters[i + 4], quarters[i + 8], quarters_names[i + 4], quarters_names[i + 8]))
    outdtf = pd.DataFrame()

    tb_col = []
    ref_col = []
    comp_col = []
    visiteurs_actifs = []
    nb_visiteurs_actifs = []

    for ref_quarter, comp_quarter, ref_quarter_name, comp_quarter_name in quarters_pairs:
        # tous tbs confondus
        tb_col.append("tous_tb")
        ref_col.append(ref_quarter_name)
        comp_col.append(comp_quarter_name)

        # extract users that came in tb in ref_month
        diff = users_came_again(df, ref_quarter, comp_quarter)
        visiteurs_actifs.append(diff)
        nb_visiteurs_actifs.append(len(diff))

        # par tb
        for tb in tbs:
            tb_df = df[df["nom_tb"] == tb]

            tb_col.append(tb)
            ref_col.append(ref_quarter_name)
            comp_col.append(comp_quarter_name)

            # extract users that came in tb in ref_month
            diff = users_came_again(tb_df, ref_quarter, comp_quarter)

            visiteurs_actifs.append(diff)
            nb_visiteurs_actifs.append(len(diff))

    outdtf["tb"] = tb_col
    outdtf["ref"] = ref_col
    outdtf["comp"] = comp_col
    outdtf["utilisateurs_revenus"] = visiteurs_actifs
    outdtf["nb_utilisateurs_revenus"] = nb_visiteurs_actifs

    return outdtf


def compare_periods(df):
    df["semaine"] = pd.to_datetime(df["semaine"], utc=True).dt.date
    df["month"] = df["semaine"].apply(get_month_from_date)

    month_df = compare_months(df)
    quarter_df = compare_quarters(df)

    out_df = pd.concat([month_df, quarter_df], ignore_index=True)
    out_df = out_df.dropna(subset=["tb"])

    return out_df


def model(dbt, session):
    df = dbt.ref("suivi_visites_tb_prive_semaine")
    outdtf = compare_periods(df)
    return outdtf
