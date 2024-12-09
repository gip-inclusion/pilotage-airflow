import glob
import os
import shutil
import subprocess
from tempfile import TemporaryDirectory

import airflow
from airflow.operators import empty, python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dags.common import default_dag_args, s3, slack
from dags.common.flux_iae import get_fluxiae_df, get_fluxiae_referential_filenames, store_df


dag_args = default_dag_args()

with airflow.DAG(
    dag_id="populate_metabase_fluxiae",
    schedule_interval=None,
    **dag_args,
) as dag:

    def create_import_directory(params=None, **kwargs):
        kwargs["ti"].xcom_push("work_directory", TemporaryDirectory().name)

    def import_and_decrypt_flux_iae(params=None, **kwargs):
        def cleanup_import_directory(workdir):
            shutil.rmtree(workdir)

        client = s3.client()
        with TemporaryDirectory() as download_dir:
            with open(f"{download_dir}/FLUX_IAE_ZIP.tar.gz", "wb") as fileobj:
                # Retrieve the encrypted and compressed file from S3 storage.
                client.download_fileobj(os.getenv("ASP_S3_BUCKET"), "FLUX_IAE_ZIP.tar.gz", fileobj)

                # Unzip the file.
                # NOTE: tar.gz extension but it is a Zip file...
                work_directory = kwargs["ti"].xcom_pull(
                    task_ids="create_flux_iae_import_directory", key="work_directory"
                )
                subprocess.check_call(["7z", "x", fileobj.name, f"-o{work_directory}"])

                # Decrypt the contents.
                for aes_encrypted_csv in glob.glob(f"{work_directory}/*.csv.gz"):
                    password = os.getenv("ASP_RIAE_UNZIP_PASSWORD")
                    subprocess.check_call(["7z", "x", f"-p{password}", aes_encrypted_csv, f"-o{work_directory}"])
                    subprocess.check_call(["rm", aes_encrypted_csv])

    def process_flux_iae(params=None, **kwargs):
        import_directory = kwargs["ti"].xcom_pull(task_ids="create_flux_iae_import_directory", key="work_directory")

        def populate_fluxiae_view(vue_name, skip_first_row=True):
            df = get_fluxiae_df(import_directory=import_directory, vue_name=vue_name, skip_first_row=skip_first_row)
            store_df(df=df, table_name=vue_name)

        def populate_fluxiae_referentials():
            for filename in get_fluxiae_referential_filenames(import_directory):
                populate_fluxiae_view(vue_name=filename)

        populate_fluxiae_referentials()

        populate_fluxiae_view(vue_name="fluxIAE_AnnexeFinanciere")
        populate_fluxiae_view(vue_name="fluxIAE_AnnexeFinanciereACI")
        populate_fluxiae_view(vue_name="fluxIAE_Convention")
        populate_fluxiae_view(vue_name="fluxIAE_ContratMission")
        populate_fluxiae_view(vue_name="fluxIAE_Encadrement")
        populate_fluxiae_view(vue_name="fluxIAE_EtatMensuelAgregat")
        populate_fluxiae_view(vue_name="fluxIAE_EtatMensuelIndiv")
        populate_fluxiae_view(vue_name="fluxIAE_Financement")
        populate_fluxiae_view(vue_name="fluxIAE_Formations")
        populate_fluxiae_view(vue_name="fluxIAE_MarchesPublics")
        populate_fluxiae_view(vue_name="fluxIAE_Missions")
        populate_fluxiae_view(vue_name="fluxIAE_MissionsEtatMensuelIndiv")
        populate_fluxiae_view(vue_name="fluxIAE_PMSMP")
        populate_fluxiae_view(vue_name="fluxIAE_Salarie")
        populate_fluxiae_view(vue_name="fluxIAE_Structure")

    def cleanup_import_directory(params=None, **kwargs):
        flux_iae_import_directory = kwargs["ti"].xcom_pull(
            task_ids="create_flux_iae_import_directory", key="work_directory"
        )
        shutil.rmtree(flux_iae_import_directory)

    start = empty.EmptyOperator(task_id="start")

    create_import_directory = python.PythonOperator(
        task_id="create_flux_iae_import_directory",
        provide_context=True,
        python_callable=create_import_directory,
    )

    import_and_decrypt_flux_iae = python.PythonOperator(
        task_id="import_and_decrypt_flux_iae",
        provide_context=True,
        python_callable=import_and_decrypt_flux_iae,
    )

    process_flux_iae = python.PythonOperator(
        task_id="process_flux_iae",
        provide_context=True,
        python_callable=process_flux_iae,
    )

    cleanup_import_directory = python.PythonOperator(
        task_id="import_flux_iae_cleanup", provide_context=True, python_callable=cleanup_import_directory
    )

    dbt_weekly = TriggerDagRunOperator(
        task_id="trigger_dbt_weekly",
        trigger_dag_id="dbt_weekly",
        wait_for_completion=True,
    )

    end = slack.success_notifying_task()

    (
        start
        >> create_import_directory
        >> import_and_decrypt_flux_iae
        >> process_flux_iae
        >> cleanup_import_directory
        >> dbt_weekly
        >> end
    )
