with candidatures as (
    select
        date_candidature,
        id_candidat,
        id_org_prescripteur,
        nom_org_prescripteur,
        "origine_détaillée",
        origine
    from {{ ref('candidatures_echelle_locale') }}
),

candidats as (
    select
        hash_nir,
        id
    from {{ ref('candidats') }}
    where hash_nir is not null
)

select distinct on (candidats.hash_nir)
    candidats.hash_nir,
    candidatures.date_candidature as date_derniere_candidature,
    candidatures.id_org_prescripteur,
    candidatures.nom_org_prescripteur,
    "origine_détaillée"           as origine_detaillee,
    candidatures.origine
from
    candidatures
left join
    candidats
    on candidatures.id_candidat = candidats.id
where candidats.hash_nir is not null
order by candidats.hash_nir asc, candidatures.date_candidature desc
