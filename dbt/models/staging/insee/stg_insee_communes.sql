with src as (
    select * from {{ ref('seed_insee_communes') }}
)

select
    typecom as type_commune,

    case
        when typecom = 'COM' then 'commune'
        when typecom = 'COMD' then 'commune_deleguee'
        when typecom = 'COMA' then 'commune_associee'
        when typecom = 'ARM' then 'arrondissement_municipal'
    end as type_commune_label,

    com::text as code_commune_insee,
    reg::text as code_region_insee,
    dep::text as code_departement_insee,
    ctcd::text as code_collectivite_territoriale_insee,
    arr::text as code_arrondissement_insee,
    tncc::text as type_nom_en_clair,
    ncc as nom_en_clair_majuscule,
    nccenr as nom_en_clair_majuscule_riche,
    libelle as libelle,
    can::text as code_canton_insee,
    comparent::text as code_commune_parente

from src
