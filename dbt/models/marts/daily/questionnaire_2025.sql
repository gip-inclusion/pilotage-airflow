{{ config(schema='esat') }}

with deduplicated as (
    select
        {{ pilo_star(source('esat', 'raw_questionnaire_2025')) }},
        -- pour ne garder que la dernière réponse dans le cas de doublons
        row_number() over (partition by esat_siret order by submitted_at desc) as row_num
    from
        {{ source('esat', 'raw_questionnaire_2025') }}
)

select *
from deduplicated
where row_num = 1
