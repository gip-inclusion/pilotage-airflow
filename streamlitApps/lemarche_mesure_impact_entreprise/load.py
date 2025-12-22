import pandas as pd
import streamlit as st
from streamlitApps.commun import db
@st.cache_data
def load_excel(file) -> pd.DataFrame:
    """
    file : fichier uploadé par Streamlit (BytesIO)
    """
    return pd.read_excel(file)

def get_marche_suivi_structure_df(siren_list, year_selected) -> pd.DataFrame:
    siren_str = ','.join(f"'{s}'" for s in siren_list)
    query = f"""
    SELECT *
    FROM public.marche_suivi_structure
    WHERE LEFT(structure_siret_signature::text,9) IN ({siren_str})
        AND emi_sme_annee =  {year_selected} ;"""
    return db.create_df_from_db(query)