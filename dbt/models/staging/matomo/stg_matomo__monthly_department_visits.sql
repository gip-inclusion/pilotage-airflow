with source as (
    select
        month::date                                                                as mois,
        id_site::integer                                                           as id_site,
        segment::text                                                              as segment,
        dimension_id::integer                                                      as dimension_id,
        department_label::text                                                     as department_label,
        nb_visits::integer                                                         as nb_visits,
        fetched_at,
        upper(trim(regexp_replace(coalesce(department_label, ''), '\s*-.*$', ''))) as department_code
    from {{ source('raw_matomo', 'matomo__actes_metier_matomo_monthly_department_visits') }}
)

select
    mois,
    id_site,
    segment,
    dimension_id,
    department_label,
    nb_visits,
    fetched_at,
    case
        when nullif(department_code, '') is null then 'Inconnu'
        when department_code in ('2A', '2B') then department_code
        when department_code ~ '^[0-9]+$' and department_code::integer between 1 and 95
            then lpad(department_code::integer::text, 2, '0')
        when department_code ~ '^[0-9]+$' and department_code::integer between 971 and 976
            then department_code::integer::text
        else 'Inconnu'
    end as departement
from
    source
