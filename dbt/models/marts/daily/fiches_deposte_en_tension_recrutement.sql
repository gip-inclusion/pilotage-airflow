{{
    config(
        materialized='incremental',
        unique_key=['id_structure', 'valeur']
    )
}}

with all_fdp as (
    select
        domaine_professionnel,
        grand_domaine,
        rome,
        "nom_département_structure",
        "département_structure",
        "région_structure",
        epci_structure,
        bassin_emploi_structure,
        type_structure,
        categorie_structure,
        id_structure,
        nom_structure,
        case
            when true then '1- Fiches de poste'
            when active = true then '2- Fiches de poste actives'
            when active = true and not candidature_30_derniers_jours or not embauche_30_derniers_jours then '3- Fiches de poste actives sans recrutement dans les 30 derniers jours'
            when active = true and not candidature_30_derniers_jours or not embauche_30_derniers_jours and not refus_30_jours_pas_de_poste then '4- Fiches de poste actives sans recrutement dans les 30 derniers jours et sans motif pas de poste ouvert'
            when active = true and not candidature_30_derniers_jours or not embauche_30_derniers_jours and not refus_30_jours_pas_de_poste and delai_mise_en_ligne >= 30 then '5- Fiches de poste en difficulté de recrutement'
            when active = true and not candidature_30_derniers_jours or not embauche_30_derniers_jours and not refus_30_jours_pas_de_poste and delai_mise_en_ligne >= 30 and aucune_candidatures_recues then '6- Fiches de poste en difficulté de recrutement n ayant jamais reçu de candidatures'
        end      as etape,
        count(*) as valeur
    from {{ ref('stg_fdp') }}
    group by
        domaine_professionnel,
        grand_domaine,
        rome,
        "nom_département_structure",
        "département_structure",
        "région_structure",
        epci_structure,
        bassin_emploi_structure,
        type_structure,
        categorie_structure,
        id_structure,
        nom_structure,
        case
            when true then '1- Fiches de poste'
            when active = true then '2- Fiches de poste actives'
            when active = true and not candidature_30_derniers_jours or not embauche_30_derniers_jours then '3- Fiches de poste actives sans recrutement dans les 30 derniers jours'
            when active = true and not candidature_30_derniers_jours or not embauche_30_derniers_jours and not refus_30_jours_pas_de_poste then '4- Fiches de poste actives sans recrutement dans les 30 derniers jours et sans motif pas de poste ouvert'
            when active = true and not candidature_30_derniers_jours or not embauche_30_derniers_jours and not refus_30_jours_pas_de_poste and delai_mise_en_ligne >= 30 then '5- Fiches de poste en difficulté de recrutement'
            when active = true and not candidature_30_derniers_jours or not embauche_30_derniers_jours and not refus_30_jours_pas_de_poste and delai_mise_en_ligne >= 30 and aucune_candidatures_recues then '6- Fiches de poste en difficulté de recrutement n ayant jamais reçu de candidatures'
        end
)

select
    domaine_professionnel,
    grand_domaine,
    rome,
    "nom_département_structure",
    "département_structure",
    "région_structure",
    epci_structure,
    bassin_emploi_structure,
    type_structure,
    categorie_structure,
    id_structure,
    nom_structure,
    etape,
    sum(valeur)                                    as valeur,
    substring(rome from position('-' in rome) + 1) as "métier"
from all_fdp
group by
    domaine_professionnel,
    grand_domaine,
    rome,
    substring(rome from position('-' in rome) + 1),
    "nom_département_structure",
    "département_structure",
    "région_structure",
    epci_structure,
    bassin_emploi_structure,
    type_structure,
    categorie_structure,
    id_structure,
    nom_structure,
    etape
