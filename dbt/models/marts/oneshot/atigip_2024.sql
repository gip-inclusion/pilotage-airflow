select
    insee_regions."LIBELLE"                                                                                       as "Région conseiller",
    org."département"                                                                                             as "Département conseiller",
    org.code_postal                                                                                               as "Code postal conseiller",
    org.nom                                                                                                       as "Nom SPIP",
    count(cel.id)                                                                                                 as "Nombre d'orientations",
    sum(case when cel."état" = 'Candidature acceptée' then 1 else 0 end)                                          as "Nombre de candidatures acceptées",
    sum(case when cel."état" = 'Candidature refusée' then 1 else 0 end)                                           as "Nombre de candidatures refusées",
    sum(case when cel."état" != 'Candidature refusée' and cel."état" != 'Candidature acceptée' then 1 else 0 end) as "Nombre de candidatures en cours"
from {{ ref('candidatures_echelle_locale') }} as cel left join {{ ref('stg_organisations') }} as org on cel.id_org_prescripteur = org.id
left join insee_departements on ltrim(org."département", '0') = insee_departements."DEP"
left join insee_regions on insee_departements."REG" = insee_regions."REG"
where date_part('year', cel.date_candidature) = 2024 and date_part('month', cel.date_candidature) between 1 and 6 and org.type = 'SPIP'
group by
    cel."nom_prénom_conseiller",
    insee_regions."LIBELLE",
    org."département",
    org.code_postal,
    org.nom
