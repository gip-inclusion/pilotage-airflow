import contextlib
from pathlib import Path

import paramiko
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from dags.common import default_dag_args, s3


DAG_ID = "dsn_data_ingest"

SFTP_REMOTE_DIR = "."
LOCAL_TMP_ROOT = Path("/tmp") / DAG_ID
S3_PREFIX_DEFAULT = "depot_dsn/raw"


@contextlib.contextmanager
def sftp_client():
    client = paramiko.SSHClient()
    # client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # for dev with the stub sftp, uncomment the line above and comment the one below
    client.load_system_host_keys()

    client.connect(
        hostname=Variable.get("GIP_MDS_SFTP_HOST"),
        port=int(Variable.get("GIP_MDS_SFTP_PORT")),
        username=Variable.get("GIP_MDS_SFTP_USER_RECUP"),
        password=Variable.get("GIP_MDS_SFTP_PASSWORD"),
    )

    try:
        with client.open_sftp() as sftp:
            yield sftp
    finally:
        client.close()


def get_latest_file(sftp):
    entries = [e for e in sftp.listdir_attr(SFTP_REMOTE_DIR) if e.filename.lower().endswith(".7z")]

    if not entries:
        raise ValueError(f"No .7z file found in SFTP directory '{SFTP_REMOTE_DIR}'")

    return max(entries, key=lambda e: e.st_mtime)


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    **default_dag_args(),
) as dag:

    @task
    def download_latest_file(**context):
        run_id = context["ti"].run_id.replace(":", "_")
        base_tmp = LOCAL_TMP_ROOT / run_id
        base_tmp.mkdir(parents=True, exist_ok=True)

        with sftp_client() as sftp:
            latest_entry = get_latest_file(sftp)

            file_name = latest_entry.filename
            path_name = str(base_tmp / file_name)

            sftp.get(file_name, path_name)

        return {
            "file_name": file_name,
            "path_name": path_name,
        }

    @task
    def backup_to_s3(download_info, **context):
        file_name = download_info["file_name"]
        local_path = download_info["path_name"]

        bucket = Variable.get("DATASTORE_S3_PILOTAGE_BUCKET_NAME")
        run_date = context["ds_nodash"]

        name_without_ext = Path(file_name).stem
        ext = Path(file_name).suffix

        key = f"{S3_PREFIX_DEFAULT}/{run_date}/{name_without_ext}_{run_date}{ext}"

        client = s3.client()
        with open(local_path, "rb") as f:
            client.upload_fileobj(f, Bucket=bucket, Key=key)

    downloaded = download_latest_file()
    backup_to_s3(downloaded)
