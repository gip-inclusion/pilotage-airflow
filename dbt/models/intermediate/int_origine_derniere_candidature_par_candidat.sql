with candidatures as (
    select
        date_candidature,
        id_candidat,
        hash_nir,
        id_org_prescripteur,
        nom_org_prescripteur,
        "origine_détaillée",
        origine
    from {{ ref('candidatures_echelle_locale') }}
)

select distinct on (candidatures.hash_nir)
    candidatures.hash_nir,
    candidatures.date_candidature as date_derniere_candidature,
    candidatures.id_org_prescripteur,
    candidatures.nom_org_prescripteur,
    "origine_détaillée"           as origine_detaillee,
    candidatures.origine
from
    candidatures
where candidatures.hash_nir is not null
order by candidatures.hash_nir asc, candidatures.date_candidature desc
