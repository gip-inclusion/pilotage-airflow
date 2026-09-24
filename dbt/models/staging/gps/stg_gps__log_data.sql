select
    user_id,
    timestamp
from {{ source('gps', 'gps_log_data') }}
