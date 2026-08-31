with structures as (
    select * from {{ ref('stg_dora__structure') }}
)

select
    id,
    structure_id_jointure_di,
    name,
    short_desc,
    full_desc,
    url,
    phone,
    email,
    address1,
    address2,
    siret,
    city_code,
    city,
    postal_code,
    latitude,
    longitude,
    department,
    parent_id,
    typology,
    slug,
    dora_url,
    data_inclusion_id,
    data_inclusion_source,
    is_obsolete
from structures
