import streamlit as st
import os
from load import load_excel
from calculs import compute_etp_financed, filter_company_data_by_structures, get_cleaned_structures_data, get_etp_financed_table_by_categories, verify_uploaded_file

COUNT_UN_ETP = int(os.getenv("COUNT_UN_ETP"))

def upload_file_section():
    st.subheader("Importer votre fichier Excel ici")
    uploaded_file = st.file_uploader(
        "Sélectionnez un fichier Excel à importer",
        type=["xlsx", "xls"]
        )
    if uploaded_file is None:
        st.stop()
    return uploaded_file

def display_file_preview(data):
    st.subheader("Aperçu du fichier uploadé")
    st.dataframe(data.head(3))

def validate_file(data):
    is_valid, errors = verify_uploaded_file(data)
    if not is_valid:
        st.error("Erreurs détectées :")
        for e in errors:
            st.write(f"• {e}")
        st.stop()

    st.text("L'aperçu ci-dessus correspond-il aux premières trois lignes du fichier uploadé ?")

    user_confirmation = st.radio(
        "Sélectionnez une option",
        options=["Oui", "Non"]
    )

    if user_confirmation == "Oui":
        st.success("Fichier validé par l'utilisateur ✅")
    elif user_confirmation == "Non":
        st.error("Veuillez réimporter le fichier en vérifiant le format des données en amont : SIREN format text et dépense et année format nombre.")
        st.stop()

def select_year(data):
    years = data["Année"].unique()
    return st.selectbox("Sélectionnez l'année à analyser", options=years)

def compute_and_display_results(clean_company_data, clean_structures_data):
    montant_financed_by_company, etp_financed_by_company, total_etp_realised, percentage_etp_financed = compute_etp_financed(clean_company_data, clean_structures_data, COUNT_UN_ETP)
    table_etp_financed = get_etp_financed_table_by_categories(total_etp_realised,etp_financed_by_company, clean_structures_data)

    st.subheader("Résultats pour votre entreprise")

    st.metric(
        label="Montant financé par l'entreprise (€)",
        value=f"{montant_financed_by_company:,.0f}".replace(",", " ")
    )

    st.metric(
        label="Structures d'insertions ayant été cofinancées",
        value=f"{clean_structures_data["siren"].nunique()}"
    )

    st.metric(
        label="ETPs financés par votre entreprise",
        value = f"{etp_financed_by_company:.2f}"
    )

    st.metric(
        label="Correspondant au pourcentage d'ETPs réalisés par les structures que vous avez cofinancés suivant:",
        value=f"{percentage_etp_financed:.2f}%"
    )

    st.subheader("""Détails des ETPs financés par votre entreprise """)
    st.markdown(f"**Exemple lecture données pour category = femmme**")
    st.markdown(f"""
                Parmi les {etp_financed_by_company:.2f} ETPs financés par l'entreprise,
                {table_etp_financed.loc[table_etp_financed['category']=='Femme', 'Nombre ETP'].values[0]:.2f}
                ETPs (correspondant au {100*table_etp_financed.loc[table_etp_financed['category']=='Femme', 'Ratio ETP'].values[0]:.2f}%) sont des femmes. Cela represents environs {table_etp_financed.loc[table_etp_financed['category']=='Femme', 'Nombre beneficiaires'].values[0]:.2f} femmes salariées.
                """)
    st.dataframe(table_etp_financed)

# Upload file
uploaded_file = upload_file_section()

# Chargement du file
data_company = load_excel(uploaded_file)

# Aperçu du fichier
display_file_preview(data_company)

# Validation du fichier
validate_file(data_company)

# Sélection de l'année
selected_year = select_year(data_company)

# Nettoyage et filtrage des données
clean_structures_data = get_cleaned_structures_data(data_company, selected_year)
clean_company_data = filter_company_data_by_structures(data_company, clean_structures_data, selected_year)


# Calcul et affichage des résultats
compute_and_display_results(clean_company_data, clean_structures_data)