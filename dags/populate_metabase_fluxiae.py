import glob
import logging
import shutil
import subprocess
import tempfile

import airflow
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from dags.common import default_dag_args, s3, slack
from dags.common.flux_iae import get_fluxiae_df, get_fluxiae_referential_filenames, store_df


logger = logging.getLogger(__name__)

with airflow.DAG(
    **default_dag_args(),
    dag_id="populate_metabase_fluxiae",
    schedule="0 13 * * 1",
) as dag:

    @task(task_id="create_work_directory")
    def create_work_directory(*, task_instance, **kwargs):
        dag_run = task_instance.get_dagrun()
        return tempfile.mkdtemp(prefix=f"{dag_run.dag_id}_", suffix=f"_{dag_run.run_id}")

    @task(task_id="import_and_decrypt")
    def import_and_decrypt(import_directory, **kwargs):
        bucket_name = Variable.get("DATASTORE_S3_EMPLOIS_BUCKET_NAME")
        client = s3.client()

        # Use the most recent file available in the bucket
        response = client.list_objects_v2(Bucket=bucket_name, Prefix="flux-iae/")
        if not response["KeyCount"]:
            raise RuntimeError("No files found in bucket")

        with tempfile.NamedTemporaryFile(dir=import_directory) as file:
            most_recent_import = max(obj["Key"] for obj in response["Contents"])
            logger.info("Most recent file is: %r", most_recent_import)

            # Check that the file is new; this DAG shouldn't run on the same file twice.
            if Variable.get("ASP_FLUX_IAE_MOST_RECENT_IMPORT", None) == most_recent_import:
                raise RuntimeError(
                    f"{most_recent_import} is the most recent FluxIAE import in the bucket, "
                    "but a file with the same key was already imported! "
                    "A new export may not have uploaded to the bucket. To re-run the DAG with the file, "
                    "clear the value of ASP_FLUX_IAE_MOST_RECENT_IMPORT from the Airflow variables."
                )

            # Retrieve the encrypted and compressed file from S3 storage.
            client.download_fileobj(bucket_name, most_recent_import, file)

            # Unzip the file.
            # NOTE: tar.gz extension but it is a Zip file...
            logger.info("Decompressing file %r into %r", file.name, import_directory)
            subprocess.check_call(["7z", "x", file.name, f"-o{import_directory}"])

        # Decrypt the contents.
        for aes_encrypted_csv in glob.glob(f"{import_directory}/*.csv.gz"):
            logger.info("Decompressing file %r into %r", aes_encrypted_csv, import_directory)
            subprocess.check_call(
                " ".join(["7z", "x", "-p${ASP_RIAE_UNZIP_PASSWORD}", aes_encrypted_csv, f"-o{import_directory}"]),
                env={"ASP_RIAE_UNZIP_PASSWORD": Variable.get("ASP_RIAE_UNZIP_PASSWORD").encode()},
                shell=True,
            )
            subprocess.check_call(["rm", aes_encrypted_csv])

        return most_recent_import

    @task(task_id="process")
    def process(imported_file_key, import_directory, **kwargs):
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

        # Process complete. Mark this run as the most recent successful import.
        logger.info(f"Populated FluxIAE. Logging {imported_file_key} to configuration")
        Variable.set("ASP_FLUX_IAE_MOST_RECENT_IMPORT", imported_file_key)

    @task(task_id="cleanup_import_directory")
    def clean_work_directory(import_directory, **kwargs):
        shutil.rmtree(import_directory)

    work_directory = create_work_directory().as_setup()
    processed = process(import_and_decrypt(work_directory), work_directory)
    # Clean the work directory after processing or as teardown
    processed >> clean_work_directory(work_directory).as_teardown(setups=work_directory, on_failure_fail_dagrun=True)
    # Trigger the DAG after the Slack notification to have a nice linear history
    (
        processed
        >> slack.success_notifying_task()
        >> TriggerDagRunOperator(
            task_id="trigger_dbt_weekly",
            trigger_dag_id="dbt_weekly",
        )
    )
