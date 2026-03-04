with source_count as (
    select count(distinct id) as cnt
    from {{ source('emplois', 'utilisateurs_v0') }}
    where "dernière_connexion" is not null
),

model_count as (
    select count(distinct user_id) as cnt
    from {{ ref('fct_logins_utilisateurs_emplois') }}
)

select
    s.cnt as source_users,
    m.cnt as model_users
from source_count as s, model_count as m
where s.cnt != m.cnt
