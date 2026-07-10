select
    id,
    name,
    number::text                 as number,
    capital,
    region,
    email,
    phone_number,
    parcours_enabled::boolean    as parcours_enabled,
    disable_ft_webhooks::boolean as disable_ft_webhooks
from {{ source('rdv_insertion', 'departments') }}
