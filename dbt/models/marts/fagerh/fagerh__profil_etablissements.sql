with etablissements as (

    select *
    from {{ ref('int_fagerh__etablissements') }}

),

final as (

    select
        uuid,
        finess,
        establishment_name,
        departement,

        has_esrp,
        has_espo,
        has_ueros,
        has_deac,

        implantation_zone,
        has_public_transport,
        is_pmr_accessible,
        has_alternative_transport,
        distance_to_prefecture,

        has_accommodation,
        accommodation_places,
        accommodation_open_weekend,
        accommodation_has_weekend_accounting,
        accommodation_has_closure_periods,
        accommodation_closure_weeks,
        accommodation_has_alternatives,

        has_collective_restaurant,
        restaurant_open_weekend,
        has_external_catering,
        has_catering_financial_participation,

        has_shared_kitchen,
        shared_kitchen_open_weekend

    from etablissements

    where is_response_completed is true

)

select *
from final
