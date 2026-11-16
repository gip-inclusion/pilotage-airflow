with invitations as (
    select * from {{ ref('stg_rdvi__invitations') }}
),

eligible_invitations as (
    select
        id,
        user_id,
        follow_up_id,
        created_at,
        created_at::date as invitation_date,
        format,
        origin,
        case
            when delivery_status = 'delivered' then true
            when delivery_status in ('soft_bounce', 'hard_bounce', 'blocked', 'invalid_email', 'error') then false
        end              as is_delivered
    from invitations
    where origin is distinct from 'reminder'
),

delivery_by_format_and_origin as (
    select
        *,
        -- IS FALSE keeps unknown deliveries from being ignored by BOOL_AND.
        bool_and(is_delivered is false) over (
            partition by user_id, follow_up_id, invitation_date, format
        ) as all_format_sends_failed,
        bool_or(is_delivered) over (
            partition by user_id, follow_up_id, invitation_date, origin
        ) as origin_has_delivery,
        bool_and(is_delivered is false) over (
            partition by user_id, follow_up_id, invitation_date, origin
        ) as all_origin_sends_failed
    from eligible_invitations
)

select
    {{ dbt_utils.generate_surrogate_key(['user_id', 'follow_up_id', 'invitation_date']) }} as invitation_id,
    user_id,
    follow_up_id,
    invitation_date,
    min(created_at)                                                                        as created_at,
    array_agg(id order by id)                                                              as source_invitation_ids,
    case
        when bool_or(is_delivered) then true
        when bool_and(is_delivered is false) then false
    end                                                                                    as is_delivered,
    {% for channel in ['email', 'sms', 'postal'] %}
        case
            when count(*) filter (where format = '{{ channel }}') = 0 then 'not_sent'
            when count(*) filter (where format = '{{ channel }}' and is_delivered) > 0 then 'delivered'
            when
                count(*) filter (where format = '{{ channel }}' and is_delivered is false)
                = count(*) filter (where format = '{{ channel }}')
                then 'failed'
            else 'unknown'
        end                                                                                as delivery_status_{{ channel }},
    {% endfor %}
    coalesce(
        array_agg(distinct format::text order by format::text)
        filter (where all_format_sends_failed and format is not null),
        array[]::text[]
    )                                                                                      as failed_formats,
    coalesce(
        array_agg(distinct origin::text order by origin::text)
        filter (where origin_has_delivery and origin is not null),
        array[]::text[]
    )                                                                                      as delivered_origins,
    coalesce(
        array_agg(distinct origin::text order by origin::text)
        filter (where all_origin_sends_failed and origin is not null),
        array[]::text[]
    )                                                                                      as failed_origins
from delivery_by_format_and_origin
group by user_id, follow_up_id, invitation_date
