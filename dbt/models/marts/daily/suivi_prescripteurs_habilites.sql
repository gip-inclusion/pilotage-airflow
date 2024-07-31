select
    {{ pilo_star(ref('stg_organisations'),
                 relation_alias = 'organisations') }},
    -- nombre de siae partenaires de l'organisation =
    -- nombre de siae qui ont reçu une candidature de ce prescripteur
    nb_siae_partenaires.nb_siae
from {{ ref('stg_organisations') }} as organisations
left join {{ ref("stg_siae_partenaires_prescripteurs") }} as nb_siae_partenaires
    on organisations.id = nb_siae_partenaires.id
where organisations.type != 'SANS-ORGANISATION' and organisations.type is not null and organisations."habilitée" = 1
