-- Un geiq peut avoir plusieurs bilans sur une même année (principal + antennes) ; on ne garde que
-- les infos du bilan créé le plus récemment pour obtenir une seule ligne par (geiq, année).
-- C'est un choix complétement arbitraire, à challenger avec Zohra à son retour ou quand on aura besoin de cette info
with assessments_ranked as (

    select
        label_geiq_id,
        campaign_year,
        label_geiq_name,
        geiq_department,
        row_number() over (
            partition by label_geiq_id, campaign_year
            order by created_at desc
        ) as rn
    from {{ ref('stg_geiq__assessments') }}

),

geiq as (

    select
        label_geiq_id,
        campaign_year,
        label_geiq_name,
        geiq_department
    from assessments_ranked
    where rn = 1

),

antennas as (

    select
        assessments.label_geiq_id,
        assessments.campaign_year,
        count(distinct contracts.antenna_department)                                           as antenna_department_nb,
        array_agg(distinct contracts.antenna_department order by contracts.antenna_department) as antenna_departments
    from {{ ref('stg_geiq__contracts') }} as contracts
    inner join {{ ref('stg_geiq__assessments') }} as assessments
        on contracts.assessment_id = assessments.id
    where contracts.antenna_department is not null
    group by assessments.label_geiq_id, assessments.campaign_year

)

select
    geiq.label_geiq_id,
    geiq.campaign_year,
    geiq.label_geiq_name,
    geiq.geiq_department,
    antennas.antenna_departments,
    coalesce(antennas.antenna_department_nb, 0) as antenna_department_nb
from geiq
left join antennas
    on
        geiq.label_geiq_id = antennas.label_geiq_id
        and geiq.campaign_year = antennas.campaign_year
