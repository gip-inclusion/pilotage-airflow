with source as (

    select *
    from {{ ref('stg_fagerh__reponses') }}

),

final as (

    select
        uuid,
        saisie_terminee as is_response_completed,
        finess_main     as finess,
        es_nom          as establishment_name,
        es_departement  as departement,

        esrp_active     as has_esrp,
        espo_active     as has_espo,
        ueros_active    as has_ueros,
        deac_active     as has_deac,

        esrp_etp,
        espo_etp,
        ueros_etp,
        deac_etp,

        q35_hebergement as has_accommodation,
        q35_places      as accommodation_places,
        q36_restaurant  as has_collective_restaurant,
        q37_cuisine     as has_shared_kitchen

    from source

)

select *
from final
