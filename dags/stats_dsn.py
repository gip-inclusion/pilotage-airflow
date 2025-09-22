import datetime
import io
import logging
import math
import pathlib
import string

import numpy as np
import pandas as pd
import paramiko
import py7zr
from airflow import DAG
from airflow.decorators import task
from airflow.models import Param, Variable

from dags.common import dbt, default_dag_args, slack
from dags.common.anonymize_sensible_data import decrypt_content
from dags.common.db import create_df_from_db


logger = logging.getLogger(__name__)

COHORT_COLUMNS = ["sexe", "trimestre", "type_structure_emplois", "ZE2020"]
FILE_BASE_COLUMNS = [
    "code_ligne",
    "code_struct",
    "nir",
    "nom",
    "prenom",
    "date_naissance",
    "siren",
    "siret",
]
FILE_EXTRA_COLUMNS = [
    "mois_sortie",
    "type_structure_emplois",
    "ZE2020",
    "donnee_partenaire_4",
    "donnee_partenaire_5",
    "donnee_partenaire_6",
    "donnee_partenaire_7",
    "donnee_partenaire_8",
    "donnee_partenaire_9",
    "donnee_partenaire_10",
    "donnee_partenaire_11",
    "donnee_partenaire_12",
    "donnee_partenaire_13",
    "donnee_partenaire_14",
    "donnee_partenaire_15",
    "donnee_partenaire_16",
    "donnee_partenaire_17",
    "donnee_partenaire_18",
    "donnee_partenaire_19",
    "donnee_partenaire_20",
]


def format_value(value):
    """Convert None/NULL values to empty string and handle other types"""
    if value is None or pd.isna(value):
        return ""
    return str(value)


def datetime_to_quarter(dt):
    return f"{dt.year}Q{math.ceil(dt.month / 3)}"


def check_dataframe_columns_exists(df, expected_columns):
    if missing_columns := set(expected_columns) - set(df.columns):
        raise RuntimeError(f"Columns {missing_columns!r} are missing from {list(df.columns)!r}")


def analyser_cohortes(df, *, threshold):
    """
    Analyse les effectifs des cohortes (sexe + trimestre + type SIAE + ZE)

    Args:
        df: DataFrame avec les données
        threshold: Seuil minimum d'effectif

    Returns:
        DataFrame avec l'analyse des cohortes
    """
    # Calculer les effectifs par cohorte
    effectifs = df.groupby(COHORT_COLUMNS).size().reset_index(name="effectif")
    effectifs["statut"] = np.where(effectifs["effectif"] >= threshold, "OK", "A_REGROUPER")

    logger.info("Total cohortes: %d", len(effectifs))
    logger.info("Cohortes OK (>= %d): %d", threshold, len(effectifs[effectifs["statut"] == "OK"]))
    logger.info("Cohortes à regrouper (< %d): %d", threshold, len(effectifs[effectifs["statut"] == "A_REGROUPER"]))

    return effectifs.sort_values("effectif")


def _cohort_key(row):
    return row["sexe"], row["trimestre"], row["type_structure_emplois"], row["ZE2020"]


def regrouper_cohortes(df, *, threshold):
    """
    Regroupe les cohortes avec trop peu de personnes

    Args :
        df : DataFrame avec les données originales
        threshold : Seuil minimum d'effectif par cohorte

    Returns :
        DataFrame avec les regroupements appliqués
    """
    check_dataframe_columns_exists(df, COHORT_COLUMNS)

    df_result = df.copy()
    # Identifier les cohortes insuffisantes
    effectifs_cohortes = df_result.groupby(COHORT_COLUMNS).size().reset_index(name="effectif")
    cohortes_insuffisantes = effectifs_cohortes[effectifs_cohortes["effectif"] < threshold]

    # Créer un set des cohortes insuffisantes pour une recherche plus rapide
    cohorts_to_regroup = {_cohort_key(row) for _, row in cohortes_insuffisantes.iterrows()}

    # Fonction pour déterminer les nouvelles valeurs
    def calculer_regroupement(row):
        if _cohort_key(row) in cohorts_to_regroup:
            return pd.Series(
                {
                    "ZE2020_finale": f"ZE_NEUTRE_{row['trimestre']}_{row['sexe']}",
                    "type_structure_finale": f"SIAE_NEUTRE_{row['trimestre']}_{row['sexe']}",
                    "regroupee": True,
                }
            )
        # Garder les valeurs originales
        return pd.Series(
            {
                "ZE2020_finale": row["ZE2020"],
                "type_structure_finale": row["type_structure_emplois"],
                "regroupee": False,
            }
        )

    # Appliquer le regroupement
    logger.info("Regroupement de %d cohortes", len(cohortes_insuffisantes))
    regroupements = df_result.apply(calculer_regroupement, axis=1)
    df_result = pd.concat([df_result, regroupements], axis=1)
    logger.info("Regroupement terminé. Colonnes: %r", list(df_result.columns))

    return df_result


def calculer_effectifs_finaux(df_regroupee):
    """
    Calcule les nouveaux effectifs après regroupement

    Args:
        df_regroupee: DataFrame après regroupement

    Returns:
        DataFrame avec les effectifs finaux calculés
    """
    final_cohort_columns = ["sexe", "trimestre", "type_structure_finale", "ZE2020_finale"]
    check_dataframe_columns_exists(df_regroupee, final_cohort_columns)

    df_result = df_regroupee.copy()
    # Supprimer toute colonne nombre_lignes_similaires existante pour éviter les conflits
    df_result = df_result.drop(columns={c for c in df_result.columns if c.startswith("nombre_lignes_similaires")})

    # Calculer les nouveaux effectifs
    nouveaux_effectifs = df_result.groupby(final_cohort_columns).size().reset_index(name="nombre_lignes_similaires")

    # Joindre avec les données
    df_final = df_result.merge(nouveaux_effectifs, on=final_cohort_columns, how="left")
    logger.info("Effectifs calculés. Colonnes finales: %r", list(df_final.columns))
    return df_final


def traitement_complet(df, *, threshold):
    """
    Fonction principale qui fait tout le traitement

    Args :
        df : DataFrame avec les données originales
        threshold : Seuil minimum d'effectif par cohorte

    Returns :
        DataFrame final avec regroupements appliqués
    """
    logger.info("=== ANALYSE INITIALE ===")
    analyser_cohortes(df, threshold=threshold)

    # Étapes du traitement
    df_avec_effectifs = calculer_effectifs_finaux(regrouper_cohortes(df, threshold=threshold))
    df_final = df_avec_effectifs[df_avec_effectifs["nombre_lignes_similaires"] >= threshold].copy()
    # Remplacer les colonnes par leurs versions finales
    df_final["type_structure_emplois"] = df_final["type_structure_finale"]
    df_final["ZE2020"] = df_final["ZE2020_finale"]

    logger.info("=== RÉSULTAT FINAL ===")
    logger.info("Lignes originales: %d", len(df))
    logger.info("Lignes finales: %d", len(df_final))
    logger.info("Lignes supprimées: %d", len(df) - len(df_final))

    if "regroupee" in df_avec_effectifs.columns:
        nb_regroupees = df_avec_effectifs[df_avec_effectifs["regroupee"]]["regroupee"].count()
        logger.info("Personnes regroupées: %d", nb_regroupees)

    # Vérification finale
    effectifs_finaux = analyser_cohortes(df_final, threshold=threshold)
    if cohorts_above_threshold := len(effectifs_finaux[effectifs_finaux["effectif"] < threshold]):
        raise RuntimeError(f"{cohorts_above_threshold} cohortes ont moins de {threshold} personnes")
    logger.info("✅ Toutes les cohortes respectent le seuil minimum")

    return df_final


def get_file_content(df, name, reference):
    logger.info("Generate file %r", name)
    buffer = io.StringIO()
    # Write the header.
    buffer.write(f"000|{name}|INCLUSION|MESUREIMPACT|{datetime.date.today():%d%m%Y}|{reference}|{len(df)}\n")
    for _, row in df.iterrows():
        partner_data_column = ";".join(format_value(row[col]) for col in FILE_EXTRA_COLUMNS)
        formatted_row = "|".join([format_value(row[col]) for col in FILE_BASE_COLUMNS] + [partner_data_column])
        buffer.write(f"{formatted_row}\n")

    content = buffer.getvalue()
    logger.info("File %r is %d lines and %d bytes", name, len(df), len(content))
    return content


def upload_to_sftp(archive, remote_name):
    with paramiko.SSHClient() as client:
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        client.connect(
            hostname=Variable.get("GIP_MDS_SFTP_HOST"),
            port=Variable.get("GIP_MDS_SFTP_PORT"),
            username=Variable.get("GIP_MDS_SFTP_USER"),
            password=Variable.get("GIP_MDS_SFTP_PASSWORD"),
            allow_agent=False,  # user/password only so no need to try keys
            look_for_keys=False,  # user/password only so no need to try keys
        )

        # SFTP
        with client.open_sftp() as sftp:
            archive.seek(0)
            sftp.putfo(archive, remote_name.encode(), file_size=archive.getbuffer().nbytes)


dag_args = default_dag_args() | {"default_args": dbt.get_default_args()}

with DAG(
    "stats_dsn",
    schedule=None,
    params={
        "period_start": Param(title="Date de début", type="string", format="date"),
        "period_end": Param(title="Date de fin", type="string", format="date"),
        "7zip": Param(
            title="Archive 7zip", type="boolean", default=True, description="Send the file inside a 7zip archive"
        ),
    },
    **dag_args,
) as dag:

    @task
    def process(*, params):
        query_template = string.Template(
            pathlib.Path(__file__).parent.joinpath("common", "stats_dsn", "raw_data_query.sql").read_text()
        )
        base_df = create_df_from_db(
            query_template.substitute(period_start=params["period_start"], period_end=params["period_end"])
        )
        base_df["trimestre"] = base_df["trimestre"].apply(datetime_to_quarter)
        base_df["ZE2020"] = pd.to_numeric(base_df["ZE2020"], errors="coerce").astype("Int64")

        processed_df = traitement_complet(base_df, threshold=5)
        processed_df["nir"] = processed_df["nir_chiffré"].apply(decrypt_content)
        processed_df = processed_df[FILE_BASE_COLUMNS + FILE_EXTRA_COLUMNS]
        check_dataframe_columns_exists(processed_df, FILE_BASE_COLUMNS + FILE_EXTRA_COLUMNS)

        base_name = f"GIPP3242.PIQDISB3.P{datetime.date.today():%y0%m%d}"
        archive_name, file_name = f"{base_name}.7z", f"{base_name}.txt"

        file_content = get_file_content(
            processed_df, file_name, reference=f"{datetime.date.fromisoformat(params['period_end']):01%m%Y}"
        )

        if params["7zip"]:
            logger.info("Generate archive %r", archive_name)
            archive_buffer = io.BytesIO()
            with py7zr.SevenZipFile(archive_buffer, "w") as archive:
                archive.writestr(
                    file_content,
                    file_name,
                )
            logger.info("Archive %r is %d bytes", archive_name, len(archive_buffer.getvalue()))
            remote_fo, remote_name = archive_buffer, archive_name
        else:
            remote_fo, remote_name = io.BytesIO(file_content.encode()), file_name

        upload_to_sftp(remote_fo, remote_name)

    process() >> slack.success_notifying_task()
