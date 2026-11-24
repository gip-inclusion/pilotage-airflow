with cas_attendus (date_jour, date_id_attendu) as (
    values
    (null::date, -1),
    (date '1989-12-31', null::integer),
    (date '1990-01-01', 19900101),
    (date '2000-02-29', 20000229),
    (date '2021-01-01', 20210101),
    (date '2045-12-31', 20451231),
    (date '2046-01-01', null::integer),
    (date '0025-06-15', null::integer),
    (date '3022-03-30', null::integer),
    ((timestamptz '2024-03-31 00:30:00+01' at time zone 'UTC')::date, 20240330),
    ((timestamptz '2024-10-27 00:30:00+02' at time zone 'UTC')::date, 20241026),
    ((timestamptz '1990-01-01 00:15:00+01' at time zone 'UTC')::date, null::integer)
)

select *
from cas_attendus
where {{ date_id_from_date('date_jour') }} is distinct from date_id_attendu
