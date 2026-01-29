with candidatures as (
    select
        date_candidature,
        id_candidat,
        id_org_prescripteur,
        nom_org_prescripteur,
        "origine_détaillée",
        origine
    from {{ ref('candidatures_echelle_locale') }}
    where injection_ai = 0
),

candidats as (
    select
        hash_nir,
        id
    from {{ ref('candidats')}}
    where hash_nir is not null and injection_ai = 0
)

select
    candidats.hash_nir,
    (array_agg(
        candidatures.id_org_prescripteur
        order by candidatures.date_candidature desc
    ))[1] as id_org_prescripteur,
    (array_agg(
        candidatures.nom_org_prescripteur
        order by candidatures.date_candidature desc
    ))[1] as nom_org_prescripteur,
    (array_agg(
        candidatures."origine_détaillée"
        order by candidatures.date_candidature desc
    ))[1] as origine_detaillee,
    (array_agg(
        candidatures.origine
        order by candidatures.date_candidature desc
    ))[1] as origine
from
    candidatures
left join
    candidats
on candidatures.id_candidat = candidats.id
group by candidats.hash_nir