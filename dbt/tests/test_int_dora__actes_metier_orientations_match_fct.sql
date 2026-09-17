with expected as (
    select count(*) as nombre_actes
    from {{ ref('fct_dora__orientations') }}
    where
        creation_date >= date_trunc('month', current_date) - interval '14 months'
        and creation_date < date_trunc('month', current_date)
),

actual as (
    select coalesce(sum(nombre_actes), 0) as nombre_actes
    from {{ ref('int_dora__actes_metier') }}
    where type_acte = 'Orientation vers service'
)

select
    actual.nombre_actes   as actual_nombre_actes,
    expected.nombre_actes as expected_nombre_actes
from actual
cross join expected
where actual.nombre_actes != expected.nombre_actes
