with reporting_count as (
    select count(*) as row_count
    from {{ ref('rpt_dora__orientations') }}
),

fact_count as (
    select count(*) as row_count
    from {{ ref('fct_dora__orientations') }}
)

select
    reporting_count.row_count as reporting_count,
    fact_count.row_count      as fact_count
from reporting_count
cross join fact_count
where reporting_count.row_count != fact_count.row_count
