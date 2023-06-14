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
