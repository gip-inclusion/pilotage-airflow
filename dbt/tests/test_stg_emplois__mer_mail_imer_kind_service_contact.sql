select
    mails.id  as mer_mail_id,
    mails.imer_id,
    imer.kind as imer_kind
from {{ ref('stg_emplois__mer_mail') }} as mails
inner join {{ ref('stg_emplois__imer') }} as imer
    on mails.imer_id = imer.id::text
where imer.kind is distinct from 'service_contact'
