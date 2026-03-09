with src as (
    select * from {{ ref('seed_insee_arrondissements') }}
)

select
    tncc::text                   as type_nom_en_clair,
    ncc                          as nom_en_clair_majuscule,
    nccenr                       as nom_en_clair_majuscule_riche,
    libelle,
    case
        when length(arr::text) < 3 then lpad(arr::text, 3, '0')
        else arr::text
    end                          as code_arrondissement_insee,
    case
        when length(dep::text) < 2 then lpad(dep::text, 2, '0')
        else dep::text
    end                          as code_departement_insee,
    lpad(reg::text, 2, '0')      as code_region_insee,
    lpad(cheflieu::text, 5, '0') as code_commune_insee_cheflieu
from src
