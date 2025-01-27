select
    premier_jour                                           as debut_periode,
    premier_jour + interval '2 month' - interval '1 day'   as fin_periode,
    date_trunc('month', premier_jour)                      as mois1,
    date_trunc('month', premier_jour + interval '1 month') as mois2
from generate_series('2023-01-01', date_trunc('month', current_date), '1 month'::interval) as premier_jour
