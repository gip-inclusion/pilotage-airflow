from io import BytesIO

from airflow import DAG
from airflow.decorators import task
from airflow.operators import empty

from dags.common import db, default_dag_args, ftp, slack


with DAG(
    "asp_data",
    schedule_interval=None,
    **default_dag_args(),
) as dag:
    start = empty.EmptyOperator(task_id="start")
    end = slack.success_notifying_task()

    @task(task_id="store_asp")
    def store_asp(**kwargs):

        import re

        import ftputil
        import pandas as pd

        host, user, password = ftp.bucket_connection()
        FILE_PREFIX = "DataON"

        with ftputil.FTPHost(host, user, password) as host_ft:
            asp_file = None
            for filename in host_ft.listdir("donnees_asp"):
                if filename.startswith(FILE_PREFIX) and filename.endswith(".xlsx"):
                    asp_file = filename

            if not asp_file:
                raise Exception("No matching files found in the FTP directory")

            # get the most recent file
            ASP_PROCESSED_DATA = f"donnees_asp/{asp_file}"

            print(f"Loading most recent file: {asp_file}")

            # Need to do this because setting the encoding on the pd.read_csv did not work
            with host_ft.open(ASP_PROCESSED_DATA, "rb") as asp_loaded_data:
                content = asp_loaded_data.read()
                # Pass binary content directly to pandas using BytesIO
                df = pd.read_excel(BytesIO(content))

                df = df.rename(
                    columns={
                        "0 - Salariés permanents en charge de l'encadrement technique des salariés en insertion - Nombre d'heures réalisées au cours de la période conventionnée": "salaries_enc_tech_heures_realisees",  # noqa: E501
                        "0 - Salariés permanents en charge de l'encadrement technique des salariés en insertion - Nombre d'ETP réalisés au cours de la période conventionnée  ": "salaries_enc_tech_etp_realises",  # noqa: E501
                        "0 - Salariés permanents en charge de l'accompagnement socioprofessionnel - Nombre d'heures réalisées au cours de la période conventionnée": "salaries_enc_soc_pro_heures_realisees",  # noqa: E501
                        "0 - Salariés permanents en charge de l'accompagnement socioprofessionnel - Nombre d'ETP réalisés au cours de la période conventionnée": "salaries_enc_soc_pro_etp_realises",  # noqa: E501
                        "0 - Travailleurs non salariés dans la structure (hors bénévoles) - Nombre d'heures réalisées au cours de la période conventionnée": "non_salaries_heures_realisees",  # noqa: E501
                        "0 - Travailleurs non salariés dans la structure (hors bénévoles) - Nombre d'ETP réalisés au cours de la période conventionnée": "non_salaries_etp_realises",  # noqa: E501
                        "0 - Prestataires externes en charge de l'encadrement technique et de l'accompagnement socioprofessionnel - Nombre d'heures réalisées au cours de la période conventionnée  ": "presta_externe_heures_realisees",  # noqa: E501
                        "0 - Prestataires externes en charge de l'encadrement technique et de l'accompagnement socioprofessionnel - Nombre d'ETP réalisés au cours de la période conventionnée": "presta_externe_etp_realises",  # noqa: E501
                    }
                )

                # Getting the af format that we use in our data
                # we remove all the spaces then everything after "A0" (0 can be another digit)
                df["numero_annexe_financiere"] = df["0 - Numéro de l'annexe / avenant"].apply(
                    lambda x: re.sub(r"\s+|A\d.*$", "", x)
                )

        df.to_sql("fluxIAE_BenefSorties_ASP", con=db.connection_engine(), if_exists="replace", index=False)

    store_asp = store_asp()

    (start >> store_asp >> end)
