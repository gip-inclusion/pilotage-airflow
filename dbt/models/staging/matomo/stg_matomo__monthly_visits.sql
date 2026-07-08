select
    month::date        as mois,
    id_site::integer   as id_site,
    segment::text      as segment,
    nb_visits::integer as nb_visits,
    fetched_at
from {{ source('raw_matomo', 'matomo__actes_metier_matomo_monthly_visits') }}
