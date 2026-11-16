with invitations as (
    select * from {{ ref('int_rdvi__invitations') }}
),

follow_ups as (
    select * from {{ ref('stg_rdvi__follow_ups') }}
),

motif_categories as (
    select * from {{ ref('stg_rdvi__motif_categories') }}
)

select
    invitations.invitation_id,
    invitations.user_id,
    invitations.follow_up_id,
    follow_ups.motif_category_id,
    motif_categories.name                                   as motif_category_name,
    invitations.created_at,
    {{ date_id_from_date("invitations.created_at::date") }} as created_date_id,
    invitations.source_invitation_ids,
    invitations.is_delivered,
    invitations.delivery_status_email,
    invitations.delivery_status_sms,
    invitations.delivery_status_postal,
    invitations.failed_formats,
    invitations.delivered_origins,
    invitations.failed_origins
from invitations
left join follow_ups
    on invitations.follow_up_id = follow_ups.id
left join motif_categories
    on follow_ups.motif_category_id = motif_categories.id
