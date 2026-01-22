from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

from dags.common import db, default_dag_args, s3, slack


with DAG(
    dag_id="ft_diag_data",
    schedule=None,
    **default_dag_args(),
) as dag:

    @task
    def get_latest_file():
        bucket_name = Variable.get("DATASTORE_S3_PILOTAGE_BUCKET_NAME")
        client = s3.client()
        response = client.list_objects_v2(Bucket=bucket_name, Prefix="diag_ft/")
        if not response.get("KeyCount"):
            raise RuntimeError("No files found in bucket")
        latest = max(response["Contents"], key=lambda x: x["LastModified"])
        return latest["Key"]

    @task
    def copy_to_postgres(file_key: str):
        import os
        import tempfile

        bucket_name = Variable.get("DATASTORE_S3_PILOTAGE_BUCKET_NAME")
        client = s3.client()

        # needed because crashes we try to import it directly
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".csv") as tmp_file:
            client.download_fileobj(bucket_name, file_key, tmp_file)
            tmp_path = tmp_file.name

        try:
            with db.MetabaseDatabaseCursor3() as (cur, conn):
                # linting checked with sqlfluff
                cur.execute("DROP TABLE IF EXISTS france_travail.data_diag_ft")
                cur.execute("""
                    CREATE TABLE france_travail.data_diag_ft (
                        mois_statistique INT,
                        c_territoire_id TEXT,
                        c_lblterritoire TEXT,
                        dptres TEXT,
                        regres TEXT,
                        categorie_statistique TEXT,
                        tranche_anciennete TEXT,
                        top_qpv TEXT,
                        top_zrr TEXT,
                        top_boe TEXT,
                        top_rsa TEXT,
                        tranche_age TEXT,
                        niveau_de_formation TEXT,
                        sexe TEXT,
                        situation_de_famille TEXT,
                        nombre_enfants_a_charge INT,
                        structure_accompagnement FLOAT,
                        parcours_accompagnement TEXT,
                        defm INT,
                        contrainte_numerique INT,
                        c_impactcontrainte_numeriq_id TEXT,
                        contrainte_mobilite INT,
                        c_impactcontrainte_mobilite_id TEXT,
                        contrainte_familiale INT,
                        c_impactcontrainte_famille_id TEXT,
                        contrainte_sante INT,
                        c_impactcontrainte_sante_id TEXT,
                        contrainte_savoir INT,
                        c_impactcontrainte_savoir_id TEXT,
                        contrainte_logement INT,
                        c_impactcontrainte_logement_id TEXT,
                        contrainte_financiere INT,
                        c_impactcontrainte_finance_id TEXT,
                        contrainte_admin_jurid INT,
                        c_impactcontrainte_adminjur_id TEXT,
                        n_nbcontraintesactives FLOAT,
                        diag TEXT
                    )
                """)

                with (
                    open(tmp_path, encoding="utf-8") as f,
                    cur.copy("COPY france_travail.data_diag_ft FROM STDIN WITH CSV HEADER DELIMITER ';'") as copy,
                ):
                    for line in f:
                        copy.write(line)

                conn.commit()
        finally:
            os.unlink(tmp_path)

    file_key = get_latest_file()
    copy_result = copy_to_postgres(file_key)
    copy_result >> slack.success_notifying_task()
