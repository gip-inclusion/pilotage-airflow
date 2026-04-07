with source as (

    select *
    from {{ ref('stg_fagerh__reponses') }}

),

final as (

    select
        uuid,
        saisie_terminee         as is_response_completed,
        finess_main             as finess,
        es_nom                  as establishment_name,
        es_departement          as departement,

        esrp_active             as has_esrp,
        espo_active             as has_espo,
        ueros_active            as has_ueros,
        deac_active             as has_deac,

        esrp_etp,
        espo_etp,
        ueros_etp,
        deac_etp,

        q32_implantation        as implantation_zone,
        q33_transports          as has_public_transport,
        q33_pmr                 as is_pmr_accessible,
        q33_alternatif          as has_alternative_transport,
        q34_prefecture          as distance_to_prefecture,

        q35_hebergement         as has_accommodation,
        q35_places              as accommodation_places,
        q35_weekend             as accommodation_open_weekend,
        q35_comptabilite_we     as accommodation_has_weekend_accounting,
        q35_fermetures          as accommodation_has_closure_periods,
        q35_fermetures_semaines as accommodation_closure_weeks,
        q35_alternatives        as accommodation_has_alternatives,

        q36_restaurant          as has_collective_restaurant,
        q36_weekend             as restaurant_open_weekend,
        q36_externe             as has_external_catering,
        q36_participation       as has_catering_financial_participation,

        q37_cuisine             as has_shared_kitchen,
        q37_weekend             as shared_kitchen_open_weekend

    from source

)

select *
from final
