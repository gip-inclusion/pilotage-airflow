with source as (

    select *
    from {{ ref('seed_esat__liste_officielle') }}

),

cleaned as (

    select
        "ET-Région Code"                                        as establishment_region_code,
        "ET-Région Libellé"                                     as establishment_region_label,
        "ET-Département Code"                                   as establishment_department_code,
        "ET-Département Libellé"                                as establishment_department_label,
        "ET-Commune Libellé"                                    as establishment_commune_label,

        nullif(
            regexp_replace(trim("Capacité autorisée agrégée"::text), '\s+', '', 'g'),
            ''
        )::integer                                              as authorized_capacity,
        lpad(nullif(trim("EJ-N°FINESS"::text), ''), 9, '0')     as managing_organization_finess,
        nullif(trim("EJ-Raison sociale"::text), '')             as managing_organization_name,
        nullif(trim("EJ-Statut juridique Code"::text), '')      as managing_organization_legal_status_code,
        nullif(trim("EJ-Statut juridique Libellé"::text), '')   as managing_organization_legal_status_label,
        lower(nullif(trim("EJ-Email"::text), ''))               as managing_organization_email,

        lpad(nullif(trim("ET-N°FINESS"::text), ''), 9, '0')     as establishment_finess_num,
        nullif(trim("ET-Raison sociale"::text), '')             as establishment_name,
        nullif(trim("ET-Adresse"::text), '')                    as establishment_address,
        lpad(nullif(trim("ET-Code Postal"::text), ''), 5, '0')  as establishment_postal_code,

        nullif(trim("ET-Type"::text), '')                       as establishment_type,
        lpad(nullif(trim("ET-SIRET"::text), ''), 14, '0')       as establishment_siret,

        lpad(nullif(trim("ET-Commune Code"::text), ''), 5, '0') as establishment_commune_code,
        nullif(trim("ET-Téléphone 1"::text), '')                as establishment_phone,

        lower(nullif(trim("ET-Email"::text), ''))               as establishment_email

    from source

)

select *
from cleaned
