import os

import streamlit
from calculs import (
    compute_etp_financed,
    filter_company_data_by_structures,
    get_cleaned_structures_data,
    get_etp_financed_table_by_categories,
    verify_uploaded_file,
)
from load import load_excel


COUNT_UN_ETP = int(os.getenv("COUNT_UN_ETP"))


def upload_file_section():
    streamlit.subheader("Importer votre fichier Excel ici")
    uploaded_file = streamlit.file_uploader("Sélectionnez un fichier Excel à importer", type=["xlsx", "xls"])
    if uploaded_file is None:
        streamlit.stop()
    return uploaded_file


def display_file_preview(data):
    streamlit.subheader("Aperçu du fichier uploadé")
    streamlit.dataframe(data.head(3))


def validate_file(data):
    is_valid, errors = verify_uploaded_file(data)
    if not is_valid:
        streamlit.error("Erreurs détectées :")
        for e in errors:
            streamlit.write(f"• {e}")
        streamlit.stop()

    streamlit.text("L'aperçu ci-dessus correspond-il aux premières trois lignes du fichier uploadé ?")

    user_confirmation = streamlit.radio("Sélectionnez une option", options=["Oui", "Non"])

    if user_confirmation == "Oui":
        streamlit.success("Fichier validé par l'utilisateur ✅")
    elif user_confirmation == "Non":
        streamlit.error(
            "Veuillez réimporter le fichier en vérifiant le format des données en amont :"
            " SIREN format text et dépense et année format nombre."
        )
        streamlit.stop()


def select_year(data):
    years = sorted(data["Année"].unique())
    return streamlit.selectbox("Sélectionnez l'année à analyser", options=years)


def compute_and_display_results(clean_company_data, clean_structures_data):
    montant_financed_by_company, etp_financed_by_company, total_etp_realised, percentage_etp_financed = (
        compute_etp_financed(clean_company_data, clean_structures_data, COUNT_UN_ETP)
    )
    table_etp_financed = get_etp_financed_table_by_categories(
        total_etp_realised, etp_financed_by_company, clean_structures_data
    )

    streamlit.subheader("Résultats pour votre entreprise")

    streamlit.metric(
        label="Montant financé par l'entreprise (€)", value=f"{montant_financed_by_company:,.0f}".replace(",", " ")
    )

    streamlit.metric(
        label="Structures d'insertions ayant été cofinancées", value=f"{clean_structures_data['siren'].nunique()}"
    )

    streamlit.metric(label="ETPs financés par votre entreprise", value=f"{etp_financed_by_company:.2f}")

    streamlit.metric(
        label="Correspondant au pourcentage d'ETPs réalisés par les structures que vous avez cofinancés suivant:",
        value=f"{percentage_etp_financed:.2f}%",
    )

    streamlit.subheader("""Détails des ETPs financés par votre entreprise """)
    streamlit.markdown("**Exemple lecture données pour category = femme**")

    femmes = table_etp_financed.loc[table_etp_financed["category"] == "Femme"]
    nombre_etp_femmes = femmes["Nombre ETP"].values[0] if not femmes.empty else 0
    ratio_femmes = femmes["Ratio ETP"].values[0] if not femmes.empty else 0
    nombre_beneficiaires_femmes = femmes["Nombre beneficiaires"].values[0] if not femmes.empty else 0
    streamlit.markdown(f"""
                Parmi les {etp_financed_by_company:.2f} ETPs financés par l'entreprise,
                {nombre_etp_femmes:.2f}
                ETPs (correspondant au {100 * ratio_femmes:.2f}%) sont des femmes.
                Cela represents environs {nombre_beneficiaires_femmes:.2f} femmes salariées.
                """)
    streamlit.dataframe(table_etp_financed)


uploaded_file = upload_file_section()

data_company = load_excel(uploaded_file)

display_file_preview(data_company)

validate_file(data_company)

selected_year = select_year(data_company)

clean_structures_data = get_cleaned_structures_data(data_company, selected_year)
clean_company_data = filter_company_data_by_structures(data_company, clean_structures_data, selected_year)


compute_and_display_results(clean_company_data, clean_structures_data)
