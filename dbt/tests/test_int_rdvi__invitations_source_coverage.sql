with invitations as (
    select * from {{ ref('stg_rdvi__invitations') }}
),

grouped_sources as (
    select
        invitations.invitation_id,
        invitations.user_id,
        invitations.follow_up_id,
        invitations.invitation_date,
        source_ids.source_invitation_id,
        count(*) over (partition by source_ids.source_invitation_id) as occurrences
    from {{ ref('int_rdvi__invitations') }} as invitations
    cross join lateral unnest(invitations.source_invitation_ids) as source_ids (source_invitation_id)
),

eligible_sources as (
    select *
    from invitations
    where origin is distinct from 'reminder'
)

select
    eligible_sources.id,
    grouped_sources.invitation_id,
    grouped_sources.source_invitation_id
from eligible_sources
full outer join grouped_sources
    on eligible_sources.id = grouped_sources.source_invitation_id
where
    eligible_sources.id is null
    or grouped_sources.source_invitation_id is null
    or grouped_sources.occurrences != 1
    or eligible_sources.user_id is distinct from grouped_sources.user_id
    or eligible_sources.follow_up_id is distinct from grouped_sources.follow_up_id
    or eligible_sources.created_at::date is distinct from grouped_sources.invitation_date
