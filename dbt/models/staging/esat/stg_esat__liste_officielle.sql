select
    "EJ-N°FINESS"                 as managing_organization_finess,
    "EJ-Raison sociale"           as managing_organization_name,
    "EJ-Statut juridique Code"    as managing_organization_legal_status_code,
    "EJ-Statut juridique Libellé" as managing_organization_legal_status_label,
    "EJ-Email"                    as managing_organization_email,

    "ET-N°FINESS"                 as establishment_finess_num,
    "ET-Raison sociale"           as establishment_name,
    "ET-Adresse"                  as establishment_address,
    "ET-Code Postal"              as establishment_postal_code,
    "ET-Type"                     as establishment_type,
    "ET-SIRET"                    as establishment_siret,
    "ET-Région Code"              as establishment_region_code,
    "ET-Région Libellé"           as establishment_region_label,
    "ET-Département Code"         as establishment_department_code,
    "ET-Département Libellé"      as establishment_department_label,
    "ET-Commune Code"             as establishment_commune_code,
    "ET-Commune Libellé"          as establishment_commune_label,
    "ET-Téléphone 1"              as establishment_phone,
    "ET-Email"                    as establishment_email,
    "Capacité autorisée agrégée"  as authorized_capacity

from {{ ref('seed_esat__liste_officielle') }}
