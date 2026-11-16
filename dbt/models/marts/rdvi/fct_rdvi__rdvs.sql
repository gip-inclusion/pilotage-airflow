with rdvs as (
    select * from {{ ref('stg_rdvi__rdvs') }}
),

motifs as (
    select * from {{ ref('stg_rdvi__motifs') }}
),

motif_categories as (
    select * from {{ ref('stg_rdvi__motif_categories') }}
),

participations as (
    select * from {{ ref('stg_rdvi__participations') }}
),

participation_counts as (
    select
        rdv_id,
        count(*)                                as participants_count,
        count(*) filter (where status = 'seen') as attended_users_count
    from participations
    group by rdv_id
)

select
    rdvs.id                                                as rdv_id,
    rdvs.starts_at,
    rdvs.time_zone,
    rdvs.status,
    rdvs.cancelled_at,
    rdvs.created_at,
    {{ date_id_from_date("rdvs.created_at::date") }}       as created_date_id,
    rdvs.created_by,
    rdvs.organisation_id,
    rdvs.motif_id,
    motifs.motif_category_id,
    motif_categories.name                                  as motif_category_name,
    motifs.collectif                                       as is_collective,
    coalesce(participation_counts.participants_count, 0)   as participants_count,
    coalesce(participation_counts.attended_users_count, 0) as attended_users_count
from rdvs
left join motifs
    on rdvs.motif_id = motifs.id
left join motif_categories
    on motifs.motif_category_id = motif_categories.id
left join participation_counts
    on rdvs.id = participation_counts.rdv_id
