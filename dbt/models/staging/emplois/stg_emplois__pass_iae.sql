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
        id                                                      as id_pass_iae,
        "date_début"                                            as date_debut,
        id_candidat,
        id_structure,
        case
            when type_nettoye = 'PASS IAE (99999)' then 'PASS IAE'
            when type_nettoye = 'Agrément PE via ITOU (non 99999)' then 'Agrément PE'
        end                                                     as type_pass,
        date_fin,
        "durée"                                                 as duree,
        {{ clean_text('type_structure') }} as type_structure,
        {{ clean_text('siret_structure') }} as siret_structure,
        {{ clean_text('nom_structure') }} as nom_structure,
        {{ clean_text('département_structure_ou_org_pe') }} as departement_structure_ou_org_pe,
        {{ clean_text('nom_département_structure_ou_org_pe') }} as nom_departement_structure_ou_org_pe,
        {{ clean_text('région_structure_ou_org_pe') }} as region_structure_ou_org_pe,
        injection_ai::boolean                                   as injection_ai,
        {{ clean_text('hash_numéro_pass_iae') }} as hash_numero_pass_iae,
        "date_mise_à_jour_metabase"                             as date_mise_a_jour_metabase
    from titres
)

select *
from final
