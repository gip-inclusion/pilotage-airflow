import pandas as pd


def add_past_and_current_users(dtf):
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
    # recover dict of tbs follow up
    df_dict = dict(tuple(dtf.groupby("nom_tb")))
    # sort by week
    df_dict = {k: v.sort_values(by="semaine") for k, v in df_dict.items()}
    df_new_dict = {}
    # get users follow up
    for k, v in df_dict.items():
        df_new_dict[k] = add_past_and_current_users(v)
    return pd.concat(df_new_dict.values())


def model(dbt, session):
    df = dbt.ref("suivi_visites_tb_prive_semaine")
    df = follow_visits(df)
    return df
