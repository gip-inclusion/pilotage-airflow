with source as (

    select *
    from {{ ref('stg_fagerh__reponses') }}

),

conditional_defs as (

    select
        source.uuid,
        conditional_def.value ->> 'id'   as conditional_id,
        conditional_def.value ->> 'name' as conditional_name

    from source

    cross join lateral jsonb_array_elements(
        case
            when
                jsonb_typeof(
                    coalesce(nullif(source.prestations_details_json, ''), '{}')::jsonb
                    #> '{__wizard_v3_state,runtime,conditionalDefs}'
                ) = 'array'
                then
                    coalesce(nullif(source.prestations_details_json, ''), '{}')::jsonb
                    #> '{__wizard_v3_state,runtime,conditionalDefs}'
            else '[]'::jsonb
        end
    ) as conditional_def (value)

),

classified as (

    select
        uuid,
        conditional_id,
        conditional_name,

        case
            when conditional_name like '%Préparer à accéder à une formation / remise à niveau savoirs de base%'
                then 'parcours_preparatoire'
            when conditional_name like '%Formation certifiante ou diplômante - Formation:%'
                then 'formation_certifiante'
            when conditional_name like '%Formation accompagnée certifiante%'
                then 'formation_accompagnee_certifiante_dfa'
            when conditional_name like '%Formation professionnalisante (ne débouchant pas sur un diplôme)%'
                then 'formation_professionnalisante_non_certifiante'
            when conditional_name like '%Formation accompagnée professionnalisante%'
                then 'formation_accompagnee_professionnalisante_non_certifiante_dfa'
        end                                                                                     as prestation_category_fine,

        case
            when conditional_name like '%Formation certifiante ou diplômante - Formation:%'
                then nullif(
                    coalesce(
                        substring(split_part(conditional_name, ' - Formation: ', 2) from '^(.*) \(Niveau .+\)$'),
                        split_part(conditional_name, ' - Formation: ', 2)
                    ),
                    ''
                )
        end                                                                                     as formation_name,

        conditional_name like '%Formation certifiante ou diplômante - Sélection des formations' as is_technical_step

    from conditional_defs

),

final as (

    select
        uuid,
        conditional_id,
        conditional_name,
        prestation_category_fine,
        formation_name,
        is_technical_step,
        prestation_category_fine is not null and is_technical_step is false as is_emploi_relevant

    from classified

)

select *
from final
