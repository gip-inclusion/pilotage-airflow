with src as (
    select * from {{ ref('seed_insee_communes') }}
)

select
    typecom                       as type_commune,

    ctcd::text                    as code_collectivite_territoriale_insee,

    tncc::text                    as type_nom_en_clair,
    ncc                           as nom_en_clair_majuscule,
    nccenr                        as nom_en_clair_majuscule_riche,
    libelle,
    can::text                     as code_canton_insee,
    lpad(reg::text, 2, '0')       as code_region_insee,
    case
        when length(dep::text) < 2 then lpad(dep::text, 2, '0')
        else dep::text
    end                           as code_departement_insee,
    case
        when length(arr::text) < 3 then lpad(arr::text, 3, '0')
        else arr::text
    end                           as code_arrondissement_insee,
    lpad(com::text, 5, '0')       as code_commune_insee,
    lpad(comparent::text, 5, '0') as code_commune_parente,
    case
        when typecom = 'COM' then 'commune'
        when typecom = 'COMD' then 'commune_deleguee'
        when typecom = 'COMA' then 'commune_associee'
        when typecom = 'ARM' then 'arrondissement_municipal'
    end                           as type_commune_label

from src
