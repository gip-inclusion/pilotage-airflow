select
    {{ pilo_star(ref('stg_candidatures'), relation_alias='candidatures') }},
    {{ pilo_star(ref('stg_org_prescripteur'), except=["id_org"], relation_alias='org_prescripteur') }},
    {{ pilo_star(ref('stg_bassin_emploi'), except=["nom_departement", "nom_region", "type_epci", "id_structure"],
    relation_alias='bassin_emploi') }},
    case
        when candidatures.injection_ai = 0 then 'Non'
        else 'Oui'
    end as reprise_de_stock_ai,
    nom_org.type_auteur_diagnostic_detaille,
    candidats.eligibilite_dispositif,
    candidats.tranche_age,
    case
        when candidats.age_selon_nir > 16 and candidats.age_selon_nir <= 25 then 'OUI'
        else 'NON'
    end as eligible_cej,
    case
        when candidats.age_selon_nir >= 57 then 'OUI'
        else 'NON'
    end as eligible_cdi_inclusion
from
    {{ ref('stg_candidatures') }} as candidatures
left join {{ ref('stg_bassin_emploi') }} as bassin_emploi
    on bassin_emploi.id_structure = candidatures.id_structure
left join {{ ref('stg_org_prescripteur') }} as org_prescripteur
    on org_prescripteur.id_org = candidatures.id_org_prescripteur
left join {{ ref('nom_prescripteur') }} as nom_org
    on nom_org.origine_detaille = candidatures."origine_détaillée"
left join {{ ref('candidats_enriched') }} as candidats
    on candidats.id = candidatures.id_candidat
