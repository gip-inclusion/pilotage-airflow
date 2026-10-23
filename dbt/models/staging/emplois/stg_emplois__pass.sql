with source as (
    select *
    from {{ source('emplois', 'pass_agréments') }}
),

titres as (
    select
        *,
        nullif(trim(replace(type, chr(160), ' ')), '') as type_nettoye
    from source
),

final as (
    select
        id                                    as id_pass,
        "date_début"                          as date_debut,
        id_candidat,
        id_structure,
        type,
        case
            when type_nettoye = 'PASS IAE (99999)' then 'PASS IAE'
            when type_nettoye = 'Agrément PE via ITOU (non 99999)' then 'Agrément PE'
        end                                   as type_pass,
        date_fin,
        "durée"                               as duree,
        type_structure,
        siret_structure,
        nom_structure,
        "département_structure_ou_org_pe"     as departement_structure_ou_org_pe,
        "nom_département_structure_ou_org_pe" as nom_departement_structure_ou_org_pe,
        "région_structure_ou_org_pe"          as region_structure_ou_org_pe,
        injection_ai::boolean                 as injection_ai,
        "hash_numéro_pass_iae"                as hash_numero_pass_iae,
        "date_mise_à_jour_metabase"           as date_mise_a_jour_metabase
    from titres
)

select *
from final
