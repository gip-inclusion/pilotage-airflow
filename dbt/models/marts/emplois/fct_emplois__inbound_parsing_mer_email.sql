with mails as (
    select *
    from {{ ref('stg_emplois__mer_mail') }}
),

imer as (
    select
        id,
        source_imer_id
    from {{ ref('fct_dora__imer') }}
    where origin_source = 'emplois'
)

select
    mails.id,
    imer.id                                                           as imer_id,
    mails.service_id                                                  as service_uid_di,
    mails.sent_at,
    mails.answered_at,
    mails.date_mise_a_jour_metabase,
    mails.sent_at::date                                               as sent_date,
    mails.answered_at is not null                                     as is_answered,
    extract(epoch from (mails.answered_at - mails.sent_at)) / 86400.0 as response_time_days
from mails
left join imer
    on mails.imer_id = imer.source_imer_id
