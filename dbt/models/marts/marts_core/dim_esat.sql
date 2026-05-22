select
    establishment_finess_num   as finess_num,
    establishment_name         as esat_name,
    establishment_address      as adresse,
    establishment_postal_code  as code_postal,
    establishment_type         as type_esat,
    establishment_siret        as siret,
    establishment_commune_code as code_commune_insee,
    establishment_phone        as telephone,
    establishment_email        as email,
    authorized_capacity        as capacite_autorisee,
    managing_organization_finess
from {{ ref('stg_esat__liste_officielle') }}
