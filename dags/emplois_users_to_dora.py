import logging

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.ssh.hooks import ssh

from dags.common import db, dbt, default_dag_args, slack


logger = logging.getLogger(__name__)

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}


with DAG("emplois_users_to_dora", schedule="@weekly", **dag_args) as dag:

    @task
    def export_users(**kwargs):
        les_emplois_users = db.create_df_from_db('SELECT * FROM "utilisateurs";')
        if les_emplois_users is None or les_emplois_users.empty:
            logger.info("No Les-Emplois users found to export.")
            return
        else:
            logger.info("Retrieved %d lines for Les-Emplois users.", len(les_emplois_users))

        cleaned_data = les_emplois_users.dropna(subset=["email"])
        data_to_store = cleaned_data.drop(["nom", "prenom"], axis=1).rename(
            columns={
                "dernière_connexion": "derniere_connexion",
                "date_mise_à_jour_metabase": "date_mise_a_jour_metabase",
            }
        )

        user = Variable.get("DORA_PGUSER")
        password = Variable.get("DORA_PGPASSWORD")
        dbname = Variable.get("DORA_PGDATABASE")

        ssh_hook = ssh.SSHHook(ssh_conn_id="dora_scalingo_ssh")

        logger.info("Tunnel creation...")
        with ssh_hook.get_tunnel() as tunnel:
            logger.info("Tunnel created")
            local_port = tunnel.local_bind_port
            url = f"postgresql://{user}:{password}@127.0.0.1:{local_port}/{dbname}"
            engine = db.create_engine(url)

            data_to_store.to_sql(
                name="les_emplois_utilisateurs",
                con=engine,
                schema="les_emplois",
                if_exists="replace",
                index=False,
            )
        logger.info("Exported %d Les-Emplois users to Dora.", len(data_to_store))

    export_users() >> slack.success_notifying_task()
