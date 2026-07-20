with src as (
    select *
    from {{ source('dora', 'users_user') }}
    where
        is_active is true
        and is_valid is true
        and is_staff is false
),

final as (
    select
        id,
        email,
        last_name,
        first_name,
        is_manager,
        departments,
        date_joined,
        last_login,
        last_service_reminder_email_sent,
        newsletter,
        main_activity,
        last_service_reminder_email_sent as last_notification_email_sent,
        departments[1]                   as department,
        last_login is not null           as is_activated
    from src
)

select *
from final
