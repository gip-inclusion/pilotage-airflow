select
    id,
    sent_at,
    answered_at,
    response_time_days
from {{ ref('fct_emplois__inbound_parsing_mer_email') }}
where answered_at < sent_at
