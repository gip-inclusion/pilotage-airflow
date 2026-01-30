import db
import pandas
import streamlit
from sqlalchemy import bindparam, text


@streamlit.cache_data
def load_excel(file) -> pandas.DataFrame:
    """
    file : fichier uploadé par Streamlit (BytesIO)
    """
    return pandas.read_excel(file)


def get_marche_suivi_structure_df(siren_list, year_selected) -> pandas.DataFrame:
    query = text("""
        SELECT *
        FROM public.marche_suivi_structure
        WHERE LEFT(structure_siret_actualise::text, 9) IN :sirens
          AND emi_sme_annee = :year
    """).bindparams(bindparam("sirens", expanding=True), bindparam("year"))
    return db.create_df_from_db(query, params={"sirens": siren_list, "year": int(year_selected)})
