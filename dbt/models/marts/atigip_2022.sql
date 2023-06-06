select
    org."région"                                                         as region_conseiller,
    org."département"                                                    as dept_conseiller,
    org.code_postal                                                      as cp_conseiller,
    org.nom                                                              as nom_spip,
    count(cel.id)                                                        as nb_orientations,
    sum(case when cel."état" = 'Candidature acceptée' then 1 else 0 end) as "nb_candidatures_acceptées",
    sum(case when cel."état" = 'Candidature refusée' then 1 else 0 end)  as "nb_candidatures_refusées"
from {{ ref('candidatures_echelle_locale') }} as cel left join {{ ref('stg_organisations') }} as org on cel.id_org_prescripteur = org.id
where date_part('year', cel.date_candidature) = 2022 and org.type = 'SPIP'
group by
    cel."nom_prénom_conseiller",
    org."région",
    org."département",
    org.code_postal,
    org.nom
