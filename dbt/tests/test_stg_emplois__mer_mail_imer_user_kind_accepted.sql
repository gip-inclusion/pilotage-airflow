select
    mails.id as mer_mail_id,
    mails.imer_id,
    imer.user_kind
from {{ ref('stg_emplois__mer_mail') }} as mails
inner join {{ ref('stg_emplois__imer') }} as imer
    on mails.imer_id = imer.id::text
where
    imer.user_kind is null
    or imer.user_kind not in ('emplois_prescriber', 'emplois_employer')
