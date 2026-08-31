with users as (
    select * from {{ ref('stg_dora__user') }}
),

users_with_imer as (
    select user_id
    from {{ ref('int_dora__imer') }}
    group by user_id
),

structure_members as (
    select
        user_id,
        min(creation_date) as first_date_as_structure_member
    from {{ ref("stg_dora__structure_member") }}
    group by user_id

)

select
    users.id,
    users.email,
    users.last_name,
    users.first_name,
    users.is_manager,
    users.departments,
    users.date_joined,
    users.last_login,
    users.last_service_reminder_email_sent,
    users.newsletter,
    users.main_activity,
    users.last_notification_email_sent,
    users.department,
    users.is_activated,
    structure_members.first_date_as_structure_member,
    users_with_imer.user_id is not null   as user_with_imer,
    structure_members.user_id is not null as user_is_structure_member
from users
left join users_with_imer
    on users.id = users_with_imer.user_id
left join structure_members
    on users.id = structure_members.user_id
