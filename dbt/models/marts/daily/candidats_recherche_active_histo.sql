select
    {{ pilo_star(ref('candidats_recherche_active_snapshot'), except=["dbt_valid_from", "dbt_valid_to"]) }},
    date_trunc('month', month_series) as mois_valide
from
    {{ ref('candidats_recherche_active_snapshot') }},
    generate_series(
        date_trunc('month', dbt_valid_from),
        -- when dbt_valid_to is empty, state is still the same today
        -- so we compute row for each month from valid_to to today
        case
            when dbt_valid_to is not null then date_trunc('month', dbt_valid_to)
            else date_trunc('month', current_date)
        end,
        interval '1 month'
    ) as month_series
