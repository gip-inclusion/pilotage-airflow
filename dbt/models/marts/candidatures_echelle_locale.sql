select
    {{ pilo_star(ref('stg_candidatures'), relation_alias='candidatures') }},
    {{ pilo_star(ref('stg_organisations'), except=["id", "date_mise_à_jour_metabase", "ville", "code_commune", "type", "date_inscription", "total_candidatures", "total_membres", "total_embauches", "date_derniere_candidature"], relation_alias='org_prescripteur') }},
    {{ pilo_star(ref('stg_bassin_emploi'), except=["nom_departement", "nom_region", "type_epci", "id_structure"], relation_alias='bassin_emploi') }},
    org_prescripteur.type                 as type_org_prescripteur,
    case
        when candidatures.injection_ai = 0 then 'Non'
        else 'Oui'
    end                                   as reprise_de_stock_ai,
    nom_org.type_auteur_diagnostic_detaille,
    candidats.sous_type_auteur_diagnostic as auteur_diag_candidat,
    candidats.type_auteur_diagnostic      as auteur_diag_candidat_detaille,
    candidats.eligibilite_dispositif,
    candidats.tranche_age,
    candidats.eligible_cej,
    candidats.sexe_selon_nir              as genre_candidat,
    candidats.eligible_cdi_inclusion,
    candidats.date_inscription            as date_inscription_candidat,
    org_prescripteur.date_inscription     as date_inscription_orga
from
    {{ ref('stg_candidatures') }} as candidatures
left join {{ ref('stg_bassin_emploi') }} as bassin_emploi
    on bassin_emploi.id_structure = candidatures.id_structure
left join {{ ref('stg_organisations') }} as org_prescripteur
    on org_prescripteur.id = candidatures.id_org_prescripteur
left join {{ ref('nom_prescripteur') }} as nom_org
    on nom_org.origine_detaille = candidatures."origine_détaillée"
left join {{ ref('candidats') }} as candidats
    on candidats.id = candidatures.id_candidat
