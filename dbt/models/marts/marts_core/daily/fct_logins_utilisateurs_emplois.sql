{{ config(
    materialized='incremental',
    unique_key=['user_id', 'last_login'],
    post_hook="
        delete from {{ this }}
        where user_id not in (
            select distinct id
            from {{ source('emplois', 'utilisateurs_v0') }}
        )
    "
) }}

with source as (

    select distinct
        id                   as user_id,
        "dernière_connexion" as last_login
    from {{ source('emplois', 'utilisateurs_v0') }}
    where "dernière_connexion" is not null

),

new_rows as (

    select *
    from source

    {% if is_incremental() %}
        where not exists (
            select 1
            from {{ this }} as t
            where
                t.user_id = source.user_id
                and t.last_login = source.last_login
        )
    {% endif %}

)

select
    user_id,
    last_login,
    current_timestamp as loaded_at
from new_rows
