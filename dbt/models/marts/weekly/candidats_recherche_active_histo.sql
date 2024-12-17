{{
    config(
        materialized='incremental',
        unique_key=['id', 'semaine_valide'],
        post_hook="""
            DELETE FROM {{ this }}
            WHERE date_derniere_candidature_acceptee is not null and delai_premiere_candidature > 30
        """
    )
}}

-- recover last known date in the table to increment for new period
with last_known_date as (
    select max(date_trunc('week', dbt_valid_to)) as lkd
    from {{ ref('candidats_recherche_active_snapshot') }}
)

select
    {{ pilo_star(ref('candidats_recherche_active_snapshot'), except=["dbt_valid_from", "dbt_valid_to"]) }},
    date_trunc('week', week_series) as semaine_valide
from
    {{ ref('candidats_recherche_active_snapshot') }}
cross join
    last_known_date
cross join
    generate_series(
        date_trunc('week', dbt_valid_from),
        -- when dbt_valid_to is empty, state is still the same today
        -- so we compute row for each week from valid_to to today
        coalesce(date_trunc('week', dbt_valid_to), last_known_date.lkd),
        interval '1 week'
    ) as week_series

{% if is_incremental() %}

-- we only update rows that have changed
-- the previous state is no longer current if :
-- 1: dbt_valid_from is bigger than last week date (this line is the new state) // dbt_valid_from is bigger than the last known date
-- 2: dbt_valid_to is bigger than last week date (this line is the last state that is now finished) // dbt_valid
    where dbt_valid_from >= last_known_date.lkd or dbt_valid_to >= last_known_date.lkd

{% endif %}
