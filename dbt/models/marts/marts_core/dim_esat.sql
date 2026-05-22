select
    establishment_finess_num,
    establishment_name,
    establishment_address,
    establishment_postal_code,
    establishment_type,
    establishment_siret,
    establishment_commune_code,
    establishment_phone,
    establishment_email,
    authorized_capacity,
    managing_organization_finess
from {{ ref('stg_esat__liste_officielle') }}
