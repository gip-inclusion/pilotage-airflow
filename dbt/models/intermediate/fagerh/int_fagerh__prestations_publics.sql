with source as (

    select
        uuid,
        prestation_key,
        prestation_key_base,
        prestation_group,
        prestation_label,
        orp_status,
        is_reliable_prestation_mapping,
        is_unmapped_prestation,
        prestation_done,

        json_coh_genre,
        json_coh_age,
        json_coh_niveau_formation_entree,
        json_coh_situation_entree,
        json_handicap_matrix

    from {{ ref('int_fagerh__prestations') }}

),

category_mapping as (

    select
        public_dimension,
        public_category_position,
        public_category_code,
        public_category_label

    from {{ ref('seed_fagerh__prestations_public_category_mapping') }}

),

cohort_arrays as (

    select
        source.uuid,
        source.prestation_key,
        source.prestation_key_base,
        source.prestation_group,
        source.prestation_label,
        source.orp_status,
        source.is_reliable_prestation_mapping,
        source.is_unmapped_prestation,
        source.prestation_done,

        cohort.public_dimension,
        cohort.values_json

    from source

    cross join lateral (
        values
        ('genre', source.json_coh_genre),
        ('age', source.json_coh_age),
        ('niveau_formation_entree', source.json_coh_niveau_formation_entree),
        ('situation_entree', source.json_coh_situation_entree)
    ) as cohort (public_dimension, values_json)

),

cohort_publics as (

    select
        cohort_arrays.uuid,
        cohort_arrays.prestation_key,
        cohort_arrays.prestation_key_base,
        cohort_arrays.prestation_group,
        cohort_arrays.prestation_label,
        cohort_arrays.orp_status,
        cohort_arrays.is_reliable_prestation_mapping,
        cohort_arrays.is_unmapped_prestation,
        cohort_arrays.prestation_done,

        cohort_arrays.public_dimension,
        null::text                            as public_subdimension,
        (array_item.ordinality - 1)::integer  as public_category_position,
        nullif(array_item.value, '')::integer as public_count

    from cohort_arrays

    cross join lateral jsonb_array_elements_text(
        coalesce(cohort_arrays.values_json, '[]'::jsonb)
    ) with ordinality as array_item (value, ordinality)

),

handicap_publics as (

    select
        source.uuid,
        source.prestation_key,
        source.prestation_key_base,
        source.prestation_group,
        source.prestation_label,
        source.orp_status,
        source.is_reliable_prestation_mapping,
        source.is_unmapped_prestation,
        source.prestation_done,

        'type_handicap'                                      as public_dimension,
        handicap_count.public_subdimension,
        (handicap_item.ordinality - 1)::integer              as public_category_position,
        nullif(handicap_count.public_count_raw, '')::integer as public_count

    from source

    cross join
        lateral jsonb_array_elements(
            coalesce(source.json_handicap_matrix, '[]'::jsonb)
        ) with ordinality as handicap_item (handicap_json, ordinality)

    cross join lateral (
        values
        ('principal', handicap_item.handicap_json ->> 'principal'),
        ('associe', handicap_item.handicap_json ->> 'associe')
    ) as handicap_count (public_subdimension, public_count_raw)

),

publics as (

    select *
    from cohort_publics

    union all

    select *
    from handicap_publics

),

final as (

    select
        publics.uuid,
        publics.prestation_key,
        publics.prestation_key_base,
        publics.prestation_group,
        publics.prestation_label,
        publics.orp_status,
        publics.is_reliable_prestation_mapping,
        publics.is_unmapped_prestation,
        publics.prestation_done,

        publics.public_dimension,
        publics.public_subdimension,
        publics.public_category_position,

        category_mapping.public_category_code,
        category_mapping.public_category_label,

        publics.public_count

    from publics

    left join category_mapping
        on
            publics.public_dimension = category_mapping.public_dimension
            and publics.public_category_position = category_mapping.public_category_position

    where publics.public_count > 0

)

select *
from final
