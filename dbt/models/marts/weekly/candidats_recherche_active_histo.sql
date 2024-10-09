{{
    config(
        materialized='incremental'
    )
}}

select
    {{ pilo_star(ref('candidats_recherche_active_snapshot'), except=["dbt_valid_from", "dbt_valid_to"]) }},
    date_trunc('week', week_series) as semaine_valide
from
    {{ ref('candidats_recherche_active_snapshot') }},
    generate_series(
        date_trunc('week', dbt_valid_from),
        -- when dbt_valid_to is empty, state is still the same today
        -- so we compute row for each week from valid_to to today
        case
            -- si valid to n'est pas null, on créée une ligne par semaine
            when dbt_valid_to is not null then date_trunc('week', dbt_valid_to)
            else date_trunc('week', current_date)
        end,
        interval '1 week'
    ) as week_series

{% if is_incremental() %}

-- we only update rows that have changed
-- the previous state is no longer current if :
-- 1: dbt_valid_from is bigger than last week date (this line is the new state)
-- 2: dbt_valid_to is bigger than last week date (this line is the last state that is now finished)
where dbt_valid_from > date_trunc('week', current_date) - interval '1 week'
or dbt_valid_to > date_trunc('week', current_date) - interval '1 week'

{% endif %}
