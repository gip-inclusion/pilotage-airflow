{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['structure_id_siae'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select
    {{ pilo_star(source('fluxIAE', 'fluxIAE_Structure')) }},
    -- l'ASP préconise l'utilisation de l'adresse administrative pour récupérer la commune de la structure
    app_geo.nom_epci                                                  as nom_epci_structure,
    app_geo.nom_region                                                as nom_region_structure,
    app_geo.code_dept                                                 as code_dept_structure,
    app_geo.nom_departement_complet                                   as nom_departement_structure,
    concat_ws('-', structure_denomination, structure_siret_actualise) as structure_denomination_unique
from
    {{ source('fluxIAE', 'fluxIAE_Structure') }} as structure
left join {{ ref('stg_insee_appartenance_geo_communes') }} as app_geo
    on ltrim(structure.structure_adresse_admin_code_insee, '0') = app_geo.code_insee
