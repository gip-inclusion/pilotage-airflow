import argparse
import datetime

import numpy as np
import pandas as pd


def get_nps(reco_dtf):
    '''
        calculates nps from dataframe containing recommandations
    '''
    prom_cnt = len(reco_dtf[reco_dtf["Recommendation"] >= 9])
    det_cnt = len(reco_dtf[reco_dtf["Recommendation"] <= 6])
    reco_cnt = len(reco_dtf)

    if len(reco_dtf) > 0:
        nps_i = (prom_cnt / reco_cnt - det_cnt / reco_cnt) * 100
    else:
        nps_i = np.nan

    return nps_i

def add_nps_row(nps_data_dic, date, tb, nps, users):
    '''
        adds a row to nps_data_dic
    '''
    nps_data_dic["date"].append(date)
    nps_data_dic["nps"].append(nps)
    nps_data_dic["tb"].append(tb)
    nps_data_dic["users"].append(users)
    return nps_data_dic

# recover date of today and all weeks from 06/2022
today = datetime.date.today()
today = today.strftime("%Y-%m-%d")
week_list = pd.date_range(start="2022-06-06", end=today, freq='W-MON')

# init dic of nps data
nps_data = {"date" : [],
            "tb"   : [],
            "nps"  : [],
            "users": []
            }

# parse recommandation file
parser = argparse.ArgumentParser(description = 'Recover hebdo NPS evolution from reco csv file.')
parser.add_argument('input_filename', type=argparse.FileType('r'))
parser.add_argument('output_filename', type=argparse.FileType('w'))

args = parser.parse_args()
suivi_satisfaction = args.input_filename

# read recommandation data as pandas dataframe
df = pd.read_csv(suivi_satisfaction)
df = df[["Recommendation", "Nom Du Tb", "Date"]]
df["Date"] = pd.to_datetime(df["Date"])

# recover list of all tbs
tbs = list(set(df["Nom Du Tb"]))

# loop over weeks
for week in week_list:
    # recover data before current date
    df_cur_week = df[df["Date"] <= week]

    # get global nps and add to data
    glob_nps = get_nps(df_cur_week)
    nps_data = add_nps_row(nps_data, week, "Tous TB", glob_nps, len(df_cur_week))

    # get PE/FT nps
    tb_pe = ["tb 169 - Taux de transformation PE",
             "tb 162 - Fiches de poste en tension PE",
             "tb 149 - Candidatures orientées PE",
             "tb 168 - Délai d'entrée en IAE"
            ]
    df_cur_week_pe = df_cur_week[df_cur_week["Nom Du Tb"].isin(tb_pe)]
    pe_nps = get_nps(df_cur_week_pe)
    nps_data = add_nps_row(nps_data, week, "tb PE", pe_nps, len(df_cur_week))

    # get specific nps for each tb
    for tb in tbs:
        # recover recommandations for given tb
        df_cur_tb = df_cur_week[df_cur_week["Nom Du Tb"] == tb]
        df_cur_tb_cur_date = df_cur_tb[df_cur_tb["Date"] <= week]
        tb_nps = get_nps(df_cur_tb_cur_date)
        nps_data = add_nps_row(nps_data, week, tb, tb_nps, len(df_cur_tb_cur_date))

df_nps = pd.DataFrame.from_dict(nps_data)

df_nps["date"] = pd.to_datetime(df_nps["date"])
df_nps["nps"] = df_nps["nps"].astype(float)
df_nps["users"] = df_nps["users"].astype(float)

df_nps.to_csv(args.output_filename)
