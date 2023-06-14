from airflow import DAG
from airflow.decorators import task
from airflow.operators import empty

from dags.common import dataframes, dates, db, default_dag_args, slack


with DAG(
    "nps_hebdo_pilotage",
    schedule_interval="@weekly",
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="create_weekly_nps")
    def create_weekly_nps(**kwargs):
        import pandas as pd

        week_list = dates.week_list()
        df = db.create_df_from_db('SELECT * FROM "suivi_satisfaction"')

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
            df_nps.append(dataframes.get_nps(df_cur_week, week_start, "Tous les TBs"))
            # NPS TB Pôle emploi
            df_cur_week_pe = df_cur_week[df_cur_week["Nom Du Tb"].isin(tb_pe)]
            df_nps.append(dataframes.get_nps(df_cur_week_pe, week_start, "TBs PE"))
            # NPS TB SIAE
            df_cur_week_siae = df_cur_week[df_cur_week["Nom Du Tb"].isin(tb_siae)]
            df_nps.append(dataframes.get_nps(df_cur_week_siae, week_start, "TBs SIAE"))
            # NPS TB DDETS
            df_cur_week_ddets = df_cur_week[df_cur_week["Nom Du Tb"].isin(tb_ddets)]
            df_nps.append(dataframes.get_nps(df_cur_week_ddets, week_start, "TBs DDETS"))

        df = pd.concat(df_nps)

        df.to_sql("suivi_hebdo_nps_tb", con=db.connection_engine(), if_exists="replace", index=False)

    create_nps_task = create_weekly_nps()

    (start >> create_nps_task >> end)
