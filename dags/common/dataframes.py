import pandas as pd


def to_buffer(df):
    from io import StringIO

    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)
    return buffer


def get_nps(reco_dtf, date, tb_name):
    import numpy as np
    import pandas as pd

    nps = {}
    # recover prom and det list of reco before given date
    promoteurs = reco_dtf[reco_dtf["Recommendation"] >= 9]
    detracteurs = reco_dtf[reco_dtf["Recommendation"] <= 6]
    nb_promoteurs = len(promoteurs)
    nb_detracteurs = len(detracteurs)
    nb_reco = len(reco_dtf)

    if len(reco_dtf) > 0:
        nps_i = (nb_promoteurs - nb_detracteurs) / nb_reco * 100
    else:
        nps_i = np.nan

    nps[date] = nps_i
    df_nps = pd.DataFrame.from_dict(nps, orient="index")
    df_nps = df_nps.reset_index()
    df_nps.rename(columns={"index": "Date", 0: "NPS"}, inplace=True)
    df_nps["tb"] = tb_name
    return df_nps


def add_past_and_current_users(dtf):
    """
    for one dataframe containing the list of weekly users for a dashboard,
    calculates ancient and new users and add them as new column of the dataframe
    """
    preceding = set()
    all_col = []
    nb_all = []
    new_col = []
    nb_new = []
    already_visited_col = []
    nb_ancient = []

    # for all row in the dtf
    # recover all preceding lines
    # concatenate users list
    for ix, row in dtf.iterrows():
        # recover visitors of this current week
        current = set(dtf.loc[ix]["liste_utilisateurs"])

        # get list of users that are new this week
        new = current.difference(preceding)
        new_col.append(list(new))
        nb_new.append(len(new))

        # get list of users that already visited previous weeks
        already_visited = preceding.intersection(current)
        already_visited_col.append(list(already_visited))
        nb_ancient.append(len(already_visited))

        # add these users to list of users that already visited
        preceding = preceding.union(current)
        all_col.append(list(preceding))
        nb_all.append(len(preceding))

    dtf["nouveaux"] = new_col
    dtf["nb_nouveaux"] = nb_new
    dtf["anciens"] = already_visited_col
    dtf["nb_anciens"] = nb_ancient
    dtf["tous"] = all_col
    dtf["nb_visiteurs_cumulé"] = nb_all
    return dtf


def follow_visits(dtf):
    """
    to be applied on the table suivi_tb_prive_semaine
    adds new and previous users by week for each dashboard
    """
    # recover dict of tbs follow up
    df_dict = dict(tuple(dtf.groupby("nom_tb")))
    # sort by week
    df_dict = {k: v.sort_values(by="semaine") for k, v in df_dict.items()}
    df_new_dict = {}
    # get users follow up
    for k, v in df_dict.items():
        df_new_dict[k] = add_past_and_current_users(v)
    return pd.concat(df_new_dict.values())
