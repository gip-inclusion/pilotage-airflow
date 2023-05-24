from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators import empty

from dags.common import dates, db, default_dag_args, slack


with DAG(
    "nps_pilotage",
    schedule_interval="@weekly",
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")

    end = slack.success_notifying_task()

    @task(task_id="create_nps")
    def create_nps(**kwargs):
        import re
        from datetime import datetime, timedelta

        import numpy as np
        import pandas as pd
        from sqlalchemy import create_engine

        for name, pub_sheet_url in Variable.get("NPS_DASHBOARD_PILOTAGE", deserialize_json=True):
            print(f"reading {name=} at {pub_sheet_url=}")
            sheet_df = pd.read_csv(pub_sheet_url)
            pd.to_datetime(sheet_df["Submitted at"], dayfirst=True, inplace=True)
            sheet_df.rename(
                columns={
                    "Submitted at": "Date",
                    "Quelle est la probabilité que vous recommandiez ce tableau de bord à un collègue, partenaire ou homologue ?": "Recommandation",
                },
                inplace=True,
            )
            sheet_df["Nom du Tb"] = re.findall("(?:TB...)", name)[0]
            sheet_df = sheet_df[["Date", "Recommandation", "Produit"]]
            df = pd.concat(sheet_df)

            replacements = {
                "TB32_": "tb 32 - Acceptés en auto-prescription",
                "TB43_": "tb 43 - Statistiques des emplois",
                "TB52_": "tb 52 - Typologie de prescripteurs",
                "TB54_": "tb 54 - Typologie des employeurs",
                "TB90_": "tb 90 - Analyse des métiers",
                "TB116": "tb 116 - Recrutement",
                "TB117": "tb 117 - Données IAE DREETS/DDETS",
                "TB118": "tb 118 - Données IAE CD",
                "TB136": "tb 136 - Prescripteurs habilités",
                "TB140": "tb 140 - ETP conventionnés",
                "TB149": "tb 149 - Candidatures orientées PE",
                "TB150": "tb 150 - Fiches de poste en tension",
                "TB160": "tb 160 - Facilitation de l'embauche DREETS/DDETS",
                "TB162": "tb 162 - Fiches de poste en tension PE",
                "TB165": "tb 165 - Recrutement SIAE",
                "TB168": "tb 168 - Délai d'entrée en IAE",
                "TB169": "tb 169 - Taux de transformation PE",
                "TB185": "tb 165 - Recrutement SIAE",
                "TB216": "tb 216 - Femmes dans l'IAE",
                "TB217": "tb 217 - Suivi pass IAE",
                "TB218": "tb 218 - Cartographie",
            }

            df["Nom Du Tb"].replace(replacements, inplace=True)

            colonnes = ["Utilité Indicateurs", "Prise De Decision Grace Au Tb", "Satisfaction Globale"]
            for col in colonnes:
                df[col] = np.nan

            df = df[
                [
                    "Utilité Indicateurs",
                    "Prise De Decision Grace Au Tb",
                    "Satisfaction Globale",
                    "Recommendation",
                    "Nom Du Tb",
                    "Date",
                ]
            ]

            df = df[(df["Date"] >= dates.start_of_previous_week) & (df["Dates"] <= dates.end_of_previous_week)]

        engine = create_engine(db.host + db.user)
        df.to_sql("suivi_satisfaction", con=engine, if_exists="append", index=False)

    create_nps_task = create_nps()

    (start >> create_nps_task >> end)
