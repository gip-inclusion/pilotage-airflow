select
    generate_series(
        date_trunc('year', current_date) - interval '3 year',
        date_trunc('year', current_date) + interval '1 year' - interval '1 day',
        '1 month'::interval
    ) + interval '1 month' - interval '1 day'
    as date_annexe
