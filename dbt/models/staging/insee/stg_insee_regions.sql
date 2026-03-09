with src as (
    select * from {{ ref('seed_insee_regions') }}
)

select
    tncc::text                   as type_nom_en_clair,
    ncc                          as nom_en_clair_majuscule,
    nccenr                       as nom_en_clair_majuscule_riche,
    libelle,
    lpad(reg::text, 2, '0')      as code_region_insee,
    lpad(cheflieu::text, 5, '0') as code_commune_insee_cheflieu
from src
