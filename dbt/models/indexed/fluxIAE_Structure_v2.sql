{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['structure_id_siae'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select distinct -- parfois l'ASP introduit des doublons, ici les élimine
    -- l'ASP préconise l'utilisation de l'adresse administrative pour récupérer la commune de la structure
    {{ pilo_star(source('fluxIAE', 'fluxIAE_Structure')) }},
    dim_commune.nom_epci                                              as nom_epci_structure,
    dim_commune.nom_zone_emploi                                       as zone_emploi_structure,
    dim_commune.nom_region                                            as nom_region_structure,
    dim_commune.code_departement_insee                                as code_dept_structure,
    dim_commune.nom_departement_complet                               as nom_departement_structure,
    concat_ws('-', structure_denomination, structure_siret_actualise) as structure_denomination_unique
from
    {{ source('fluxIAE', 'fluxIAE_Structure') }} as structure
left join {{ ref('dim_commune') }}
    on ltrim(structure.structure_adresse_admin_code_insee, '0') = dim_commune.code_commune_insee
