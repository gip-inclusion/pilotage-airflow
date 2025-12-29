import contextlib
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import paramiko
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from slugify import slugify

from dags.common import db, default_dag_args, s3

DAG_ID = "dsn_data_ingest"

SFTP_REMOTE_DIR = "."
LOCAL_TMP_ROOT = Path("/tmp") / DAG_ID
S3_PREFIX_DEFAULT = "depot_dsn/raw"
RAW_SCHEMA = "raw_dsn"


FILE_TABLE_MAPPING = {
    "01. Critère Géographique et Sexe.xlsx": "dsn_geographie_et_sexe",
    "02. Critère Géographique et Nature de Contrat.xlsx": "dsn_geographie_et_nature_contrat",
    "03. Critère Géographique et Secteur d'Activité.xlsx": "dsn_geographie_et_secteur_activite",
    "04. Type de SIAE et Sexe.xlsx": "dsn_type_siae_et_sexe",
    "05. Type de SIAE et Nature de Contrat.xlsx": "dsn_type_siae_et_nature_contrat",
    "06. Type de SIAE et Secteur d'Activité.xlsx": "dsn_type_siae_et_secteur_activite",
    "07. Type de SIAE et Critère Géographique.xlsx": "dsn_type_siae_et_geographie",
}


@contextlib.contextmanager
def sftp_client():
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

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


def read_dsn_excel(file_path):
    """
    The source Excel files use a two-row header.

    The first four columns are defined on the second row, while the remaining columns
    are defined on the first one. This function needs to rebuild a single row header.
    """
    raw = pd.read_excel(file_path, header=None)
    header = raw.iloc[0].fillna(raw.iloc[1])
    df = raw.iloc[2:].reset_index(drop=True)
    df.columns = header
    return df


def clean_dsn_df(df):
    """
    Column names are normalized so they are easy to read and use in SQL, without spaces,
    accents or ambiguous formatting.

    Second, the source Excel files rely on merged cells, which produces many missing
    values. Forward-filling restores the intended structure by propagating values until
    a new one appears.
    """
    df.columns = [slugify(c, separator="_") for c in df.columns]
    df[df.columns[:3]] = df[df.columns[:3]].ffill()
    return df


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    **default_dag_args(),
) as dag:

    @task
    def download_latest_file(**context):
        print("START download_latest_file")

        run_id = context["ti"].run_id.replace(":", "_")
        base_tmp = LOCAL_TMP_ROOT / run_id
        base_tmp.mkdir(parents=True, exist_ok=True)

        with sftp_client() as sftp:
            latest_entry = get_latest_file(sftp)

            file_name = latest_entry.filename
            local_path = base_tmp / file_name

            print(f"Downloading {file_name=} {local_path=}")
            sftp.get(file_name, str(local_path))

        print("END download_latest_file")
        return {
            "file_name": file_name,
            "local_path": str(local_path),
        }

    @task
    def backup_to_s3(download_info, **context):
        print("START backup_to_s3")

        file_name = download_info["file_name"]
        local_path = download_info["local_path"]

        bucket = Variable.get("DATASTORE_S3_PILOTAGE_BUCKET_NAME")
        run_date = context["ds_nodash"]

        name_without_ext = Path(file_name).stem
        ext = Path(file_name).suffix
        key = f"{S3_PREFIX_DEFAULT}/{run_date}/{name_without_ext}_{run_date}{ext}"

        print(f"Uploading to s3://{bucket}/{key}")

        client = s3.client()
        with open(local_path, "rb") as f:
            client.upload_fileobj(f, Bucket=bucket, Key=key)

        print("END backup_to_s3")

    @task
    def extract_excels(download_info):
        print("START extract_excels")

        import py7zr

        archive_path = Path(download_info["local_path"])
        extract_dir = archive_path.parent

        with py7zr.SevenZipFile(archive_path, mode="r") as archive:
            xlsx_names = [n for n in archive.getnames() if n.lower().endswith(".xlsx")]
            if not xlsx_names:
                raise ValueError("No Excel files found in archive")

            archive.extract(path=extract_dir, targets=xlsx_names)

        excel_paths = [str(extract_dir / n) for n in xlsx_names]

        extracted_excel_count = len(excel_paths)
        print(f"Extracted {extracted_excel_count=} excel files into {extract_dir=}")
        print("END extract_excels")
        return excel_paths

    @task
    def validate_expected_files(excel_paths):
        print("START validate_expected_files")

        extracted_files = [Path(p).name for p in excel_paths]
        extracted_set = set(extracted_files)
        expected_set = set(FILE_TABLE_MAPPING.keys())

        missing = expected_set - extracted_set
        extra = extracted_set - expected_set

        if missing or extra:
            raise ValueError(f"Validation failed. Missing={sorted(missing)} Extra={sorted(extra)}")

        print("END validate_expected_files")
        return excel_paths

    @task
    def load_raw_excel(file_path):
        filename = Path(file_path).name
        table_name = FILE_TABLE_MAPPING[filename]
        db_table = f"{RAW_SCHEMA}.{table_name}"

        print(f"START load_raw_excel {filename=} {db_table=}")

        df = read_dsn_excel(file_path)
        df = clean_dsn_df(df)

        df["source_file"] = filename
        df["ingested_at"] = datetime.now(UTC).isoformat()

        engine = db.connection_engine()
        df.to_sql(
            table_name,
            con=engine,
            schema=RAW_SCHEMA,
            if_exists="replace",
            index=False,
        )

        inserted_rows = len(df)
        print(f"Loaded {inserted_rows=} rows into {db_table=}")

    downloaded = download_latest_file()
    backup_to_s3(downloaded)
    excels = extract_excels(downloaded)
    validated = validate_expected_files(excels)
    load_raw_excel.expand(file_path=validated)
