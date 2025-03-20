import csv
import hashlib
import os
from pathlib import Path

import numpy as np
import pandas as pd
from psycopg import sql

from dags.common.db import MetabaseDatabaseCursor3


PANDA_DATAFRAME_TO_PSQL_TYPES_MAPPING = {
    np.int64: "bigint",
    np.object_: "text",
    np.float64: "double precision",
    np.bool_: "boolean",
}


def create_table(table_name: str, columns: list[str, str], reset=False):
    """Create table from columns names and types"""
    with MetabaseDatabaseCursor3() as (cursor, conn):
        if reset:
            cursor.execute(sql.SQL("DROP TABLE IF EXISTS {table_name}").format(table_name=sql.Identifier(table_name)))
        create_table_query = sql.SQL("CREATE TABLE IF NOT EXISTS {table_name} ({fields_with_type})").format(
            table_name=sql.Identifier(table_name),
            fields_with_type=sql.SQL(",").join(
                [sql.SQL(" ").join([sql.Identifier(col_name), sql.SQL(col_type)]) for col_name, col_type in columns]
            ),
        )
        cursor.execute(create_table_query)
        conn.commit()


def rename_table_atomically(from_table_name, to_table_name):
    """
    Rename from_table_name to to_table_name.
    Most of the time, we replace an existing table, so we will first rename
    to_table_name to z_old_<to_table_name>.
    This allows us to take our time filling the new table without locking the current one.
    Note that when the old table z_old_<to_table_name> is deleted, all its obsolete airflow staging views
    are deleted as well, they will be rebuilt by the next run of the airflow DAG `dbt_daily`.
    """

    with MetabaseDatabaseCursor3() as (cur, conn):
        # CASCADE will drop airflow staging views (e.g. stg_structures) as well.
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(get_old_table_name(to_table_name)))
        )
        conn.commit()
        cur.execute(
            sql.SQL("ALTER TABLE IF EXISTS {} RENAME TO {}").format(
                sql.Identifier(to_table_name),
                sql.Identifier(get_old_table_name(to_table_name)),
            )
        )
        cur.execute(
            sql.SQL("ALTER TABLE {} RENAME TO {}").format(
                sql.Identifier(from_table_name),
                sql.Identifier(to_table_name),
            )
        )
        conn.commit()
        # CASCADE will drop airflow staging views (e.g. stg_structures) as well.
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(get_old_table_name(to_table_name)))
        )
        conn.commit()


def get_filename(import_directory, filename_prefix, filename_extension, description=None):
    """
    Find the file in the import directory and return the path
    """
    if description is None:
        description = filename_prefix

    filenames = []
    extensions = (filename_extension, f"{filename_extension}.gz", f"{filename_extension}.riae")
    for filename in os.listdir(import_directory):
        if filename.startswith(f"{filename_prefix}_") and filename.endswith(extensions):
            filenames.append(filename)

    if len(filenames) == 0:
        raise RuntimeError(f"No match found for {description}")
    if len(filenames) > 1:
        raise RuntimeError(f"Too many matches for {description}")
    assert len(filenames) == 1

    filename = filenames[0]
    print(f"Selected file {filename} for {description}.")
    return os.path.join(import_directory, filename)


def get_fluxiae_referential_filenames(import_directory):
    known_and_expected_prefixes = {
        "fluxIAE_RefCategorieJuridique",
        "fluxIAE_RefCategorieSort",
        "fluxIAE_RefCotisationEiti",
        "fluxIAE_RefDepartement",
        "fluxIAE_RefDispositif",
        "fluxIAE_RefDureeAllocationEmploi",
        "fluxIAE_RefDureePoleEmploi",
        "fluxIAE_RefEtatAvenant",
        "fluxIAE_RefEtatSuiviMensuel",
        "fluxIAE_RefFinanceur",
        "fluxIAE_RefFormeContrat",
        "fluxIAE_RefGroupePaysPmsmp",
        "fluxIAE_RefIdcc",
        "fluxIAE_RefMesure",
        "fluxIAE_RefMontantIae",
        "fluxIAE_RefMotifRejet",
        "fluxIAE_RefMotifSort",
        "fluxIAE_RefNatureAction",
        "fluxIAE_RefNiveauFormation",
        "fluxIAE_RefObjectifFormation",
        "fluxIAE_RefObjetEcheance",
        "fluxIAE_RefObjPrincipalPmsmp",
        "fluxIAE_RefOrienteur",
        "fluxIAE_RefSecteurActivite",
        "fluxIAE_RefTypeAide",
        "fluxIAE_RefTypeElementPaiement",
        "fluxIAE_RefTypeEmployeur",
        "fluxIAE_RefTypeFormation",
        "fluxIAE_RefTypeOperation",
        "fluxIAE_RefTypeVersement",
    }
    filename_prefixes = {
        # Example of raw filename: fluxIAE_RefCategorieJuridique_29032021_090124.csv.gz
        # Let's drop the digits and keep the first relevant part only.
        "_".join(filename.split("_")[:2])
        for filename in os.listdir(import_directory)
        if filename.startswith("fluxIAE_Ref")
    }

    if filename_prefixes != known_and_expected_prefixes:
        raise RuntimeError(
            f"{len(known_and_expected_prefixes)} files expected but {len(filename_prefixes)} were found: "
            f"{filename_prefixes ^ known_and_expected_prefixes}"
        )

    return filename_prefixes


def get_new_table_name(table_name):
    return f"z_new_{table_name}"


def get_old_table_name(table_name):
    return f"z_old_{table_name}"


def hash_content(content):
    return hashlib.sha256(f'{content}{os.getenv("HASH_SALT")}'.encode()).hexdigest()


def anonymize_fluxiae_df(df):
    """
    Drop and/or anonymize sensitive data in fluxIAE dataframe.
    """
    if "salarie_date_naissance" in df.columns.tolist():
        df["salarie_annee_naissance"] = df.salarie_date_naissance.str[-4:].astype(int)

    if "salarie_agrement" in df.columns.tolist():
        df["hash_numéro_pass_iae"] = df["salarie_agrement"].apply(hash_content)
    if "salarie_nir" in df.columns.tolist():
        df["hash_nir"] = df["salarie_nir"].apply(hash_content)

    # Any column having any of these keywords inside its name will be dropped.
    # E.g. if `courriel` is a deletable keyword, then columns named `referent_courriel`,
    # `representant_courriel` etc will all be dropped.
    deletable_keywords = [
        "courriel",
        "telephone",
        "prenom",
        "nom_usage",
        "nom_naissance",
        "responsable_nom",
        "urgence_nom",
        "referent_nom",
        "representant_nom",
        "date_naissance",
        "adr_mail",
        "nationalite",
        "titre_sejour",
        "observations",
        "salarie_agrement",
        "salarie_nir",
        "salarie_adr_point_remise",
        "salarie_adr_cplt_point_geo",
        "salarie_adr_numero_voie",
        "salarie_codeextensionvoie",
        "salarie_codetypevoie",
        "salarie_adr_libelle_voie",
        "salarie_adr_cplt_distribution",
        "salarie_adr_qpv_nom",
        # Sensitive banking information.
        "bban",  # Basic Bank Account Number.
        "bic",  # Bank code.
        "nom_bqe",  # Bank name.
    ]

    for column_name in df.columns.tolist():
        for deletable_keyword in deletable_keywords:
            if deletable_keyword in column_name:
                del df[column_name]

    # Better safe than sorry when dealing with sensitive data!
    for column_name in df.columns.tolist():
        for deletable_keyword in deletable_keywords:
            assert deletable_keyword not in column_name

    return df


def infer_columns_from_df(df):
    # Generate a dataframe with the same headers a the first non null value for each column
    df_columns = [df[column_name] for column_name in df.columns]
    non_null_values = [df_column.get(df_column.first_valid_index()) for df_column in df_columns]
    initial_line = pd.DataFrame([non_null_values], columns=df.columns)

    # Generate table sql definition from np types
    return [
        (col_name, PANDA_DATAFRAME_TO_PSQL_TYPES_MAPPING[col_type.type])
        for col_name, col_type in initial_line.dtypes.items()
    ]


def store_df(df, table_name, max_attempts=5):
    """
    Store dataframe in database.

    Do this chunk by chunk to solve
    psycopg.OperationalError "server closed the connection unexpectedly" error.

    Try up to `max_attempts` times.
    """
    # Drop unnamed columns
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    # Recipe from https://stackoverflow.com/questions/44729727/pandas-slice-large-dataframe-in-chunks
    rows_per_chunk = 10 * 1000
    df_chunks = [df[i : i + rows_per_chunk] for i in range(0, df.shape[0], rows_per_chunk)]

    print(f"Storing {table_name} in {len(df_chunks)} chunks of (max) {rows_per_chunk} rows each ...")

    attempts = 0

    new_table_name = get_new_table_name(table_name)
    while attempts < max_attempts:
        try:
            columns = infer_columns_from_df(df)
            create_table(new_table_name, columns, reset=True)
            for df_chunk in df_chunks:
                rows = df_chunk.replace({np.nan: None}).to_dict(orient="split")["data"]
                with MetabaseDatabaseCursor3() as (cursor, conn):
                    with cursor.copy(
                        sql.SQL("COPY {table_name} FROM STDIN WITH (FORMAT BINARY)").format(
                            table_name=sql.Identifier(new_table_name),
                            Fields=sql.SQL(",").join(
                                [sql.Identifier(col[0]) for col in columns],
                            ),
                        ),
                    ) as copy:
                        copy.set_types([col[1] for col in columns])
                        for row in rows:
                            copy.write_row(row)
                    conn.commit()
            break
        except Exception as e:
            # Catching all exceptions is a generally a code smell but we eventually reraise it so it's ok.
            attempts += 1
            print(f"Attempt #{attempts} failed with exception {repr(e)}.")
            if attempts == max_attempts:
                print("No more attemps left, giving up and raising the exception.")
                raise
            print("New attempt started...")

    rename_table_atomically(new_table_name, table_name)
    print(f"Stored {table_name} in database ({len(df)} rows).")
    print("")


def get_fluxiae_df(
    import_directory,
    vue_name,
    converters=None,
    description=None,
    parse_dates=None,
    skip_first_row=True,
    anonymize_sensitive_data=True,
):
    """
    Load pre-decrypted fluxIAE CSV files as a dataframe.
    Any sensitive data will be dropped or anonymized.
    """
    # Prepare parameters for pandas.read_csv method.
    kwargs = {}

    if skip_first_row:
        # Some fluxIAE exports have a leading "DEB***" row, some don't.
        kwargs["skiprows"] = 1

    # All fluxIAE exports have a final "FIN***" row which should be ignored. The most obvious way to do this is
    # to use `skipfooter=1` option in `pd.read_csv` however this causes several issues:
    # - it forces the use of the 'python' engine instead of the default 'c' engine
    # - the 'python' engine is much slower than the 'c' engine
    # - the 'python' engine does not play well when faced with special characters (e.g. `"`) inside a row value,
    #   it will break or require the `error_bad_lines=False` option to ignore all those rows

    # Thus we decide to always use the 'c' engine and implement the `skipfooter=1` option ourselves by counting
    # the rows in the CSV file beforehands instead. Always using the 'c' engine is proven to significantly reduce
    # the duration and frequency of the developer's headaches.

    extracted = Path(
        get_filename(
            import_directory=import_directory,
            filename_prefix=vue_name,
            filename_extension=".csv",
            description=description,
        )
    )

    # Ignore 3 rows: the `DEB*` first row, the headers row, and the `FIN*` last row.
    nrows = len(extracted.read_text().splitlines()) - 3

    print(f"Loading {nrows} rows for {vue_name} ...")

    if converters:
        kwargs["converters"] = converters

    if parse_dates:
        kwargs["parse_dates"] = parse_dates

    # When guessing date formats, we are more likely to end up with European style dates than American in ASP files
    kwargs["dayfirst"] = True

    df = pd.read_csv(
        extracted,
        sep="|",
        # Some rows have a single `"` in a field, for example in fluxIAE_Mission the mission_descriptif field of
        # the mission id 1003399237 is `"AIEHPAD` (no closing double quote). This screws CSV parsing big time
        # as the parser will read many rows until the next `"` and consider all of them as part of the
        # initial mission_descriptif field value o_O. Let's just disable quoting alltogether to avoid that.
        quoting=csv.QUOTE_NONE,
        nrows=nrows,
        **kwargs,
        # Fix DtypeWarning (Columns have mixed types) and avoid error when field value in later rows contradicts
        # the field data format guessed on first rows.
        low_memory=False,
    )

    # If there is only one column, something went wrong, let's break early.
    # Most likely an incorrect skip_first_row value.
    assert len(df.columns.tolist()) >= 2

    assert len(df) == nrows

    if anonymize_sensitive_data:
        df = anonymize_fluxiae_df(df)

    return df
