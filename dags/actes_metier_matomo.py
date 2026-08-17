import pendulum
import sqlalchemy
from airflow.providers.standard.operators import bash
from airflow.sdk import DAG, Variable, task

from dags.common import db, dbt, default_dag_args, matomo, slack


# Valeurs reprises des scripts actes_metier de Pierre : site et segments Matomo
# utilisés pour compter les visites des pages de résultats de recherche Emplois,
# avec la dimension personnalisée 4 pour le drill down par département
ID_SITE_EMPLOIS = matomo.PROJECTS_SITE_ID["emplois"]
EMPLOYERS_RESULTS_SEGMENT = "pageUrl=@/search/employers/results"
SERVICES_RESULTS_SEGMENT = "pageUrl=@/search/services/results"
DEPARTMENT_DIMENSION_ID = 4
MONTHS_TO_FETCH = 14

dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}


CREATE_TABLES_SQL = """

CREATE TABLE IF NOT EXISTS raw.matomo__actes_metier_matomo_monthly_visits (
    month DATE NOT NULL,
    id_site INTEGER NOT NULL,
    segment TEXT NOT NULL,
    nb_visits INTEGER NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    PRIMARY KEY (month, id_site, segment)
);

CREATE TABLE IF NOT EXISTS raw.matomo__actes_metier_matomo_monthly_department_visits (
    month DATE NOT NULL,
    id_site INTEGER NOT NULL,
    segment TEXT NOT NULL,
    dimension_id INTEGER NOT NULL,
    department_label TEXT NOT NULL,
    nb_visits INTEGER NOT NULL,
    fetched_at TIMESTAMP NOT NULL,
    PRIMARY KEY (month, id_site, segment, dimension_id, department_label)
);
"""

UPSERT_MONTHLY_VISITS_SQL = """
INSERT INTO raw.matomo__actes_metier_matomo_monthly_visits (
    month,
    id_site,
    segment,
    nb_visits,
    fetched_at
)
VALUES (
    :month,
    :id_site,
    :segment,
    :nb_visits,
    :fetched_at
)
ON CONFLICT (month, id_site, segment)
DO UPDATE SET
    nb_visits = EXCLUDED.nb_visits,
    fetched_at = EXCLUDED.fetched_at;
"""

UPSERT_DEPARTMENT_VISITS_SQL = """
INSERT INTO raw.matomo__actes_metier_matomo_monthly_department_visits (
    month,
    id_site,
    segment,
    dimension_id,
    department_label,
    nb_visits,
    fetched_at
)
VALUES (
    :month,
    :id_site,
    :segment,
    :dimension_id,
    :department_label,
    :nb_visits,
    :fetched_at
)
ON CONFLICT (month, id_site, segment, dimension_id, department_label)
DO UPDATE SET
    nb_visits = EXCLUDED.nb_visits,
    fetched_at = EXCLUDED.fetched_at;
"""


def closed_months_to_fetch(months_to_fetch=MONTHS_TO_FETCH):
    last_closed_month = pendulum.now("Europe/Paris").start_of("month").subtract(months=1)
    first_month = last_closed_month.subtract(months=months_to_fetch - 1)
    return [first_month.add(months=i).date() for i in range(months_to_fetch)]


def fetch_actes_metier_matomo_data_for_site(
    matomo_base_url,
    token,
    site_id,
    monthly_segments,
    department_segment,
    department_dimension_id,
    months,
    fetched_at,
):
    monthly_visits = []
    department_visits = []

    for month in months:
        monthly_visits.extend(
            {
                "month": month,
                "id_site": site_id,
                "segment": segment,
                "nb_visits": matomo.get_monthly_visits(matomo_base_url, token, site_id, segment, month),
                "fetched_at": fetched_at,
            }
            for segment in monthly_segments
        )

        department_visits.extend(
            {
                "month": month,
                "id_site": site_id,
                "segment": department_segment,
                "dimension_id": department_dimension_id,
                "department_label": row["department_label"],
                "nb_visits": row["nb_visits"],
                "fetched_at": fetched_at,
            }
            for row in matomo.get_monthly_custom_dimension_visits(
                matomo_base_url,
                token,
                site_id,
                department_segment,
                month,
                department_dimension_id,
            )
        )

    return monthly_visits, department_visits


with DAG(
    "actes_metier_matomo",
    schedule="@monthly",
    **dag_args,
) as dag:
    env_vars = db.connection_envvars()

    @task
    def fetch_actes_metier_matomo_data():
        matomo_base_url = Variable.get("MATOMO_BASE_URL")
        token = Variable.get("MATOMO_ACTES_METIER_TOKEN")
        fetched_at = pendulum.now("UTC").naive()

        monthly_visits, department_visits = fetch_actes_metier_matomo_data_for_site(
            matomo_base_url,
            token,
            site_id=ID_SITE_EMPLOIS,
            monthly_segments=(EMPLOYERS_RESULTS_SEGMENT, SERVICES_RESULTS_SEGMENT),
            department_segment=EMPLOYERS_RESULTS_SEGMENT,
            department_dimension_id=DEPARTMENT_DIMENSION_ID,
            months=closed_months_to_fetch(),
            fetched_at=fetched_at,
        )

        with db.connection_engine().begin() as conn:
            conn.execute(sqlalchemy.text(CREATE_TABLES_SQL))
            conn.execute(sqlalchemy.text(UPSERT_MONTHLY_VISITS_SQL), monthly_visits)
            if department_visits:
                conn.execute(sqlalchemy.text(UPSERT_DEPARTMENT_VISITS_SQL), department_visits)

    dbt_debug = bash.BashOperator(
        task_id="dbt_debug",
        bash_command="dbt debug",
        env=env_vars,
        append_env=True,
    )

    dbt_deps = bash.BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps",
        env=env_vars,
        append_env=True,
    )

    dbt_build = bash.BashOperator(
        task_id="dbt_build",
        bash_command='dbt build --select "+tag:matomo-actes-metier"',
        env=env_vars,
        append_env=True,
    )

    fetched = fetch_actes_metier_matomo_data()

    fetched >> dbt_debug >> dbt_deps >> dbt_build >> slack.success_notifying_task()
