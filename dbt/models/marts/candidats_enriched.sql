select
    {{ pilo_star(ref('stg_candidats')) }},
    case
        when candidats.age_selon_nir < 25 then 'jeune (-25 ans)'
        when candidats.age_selon_nir > 25 and candidats.age_selon_nir < 55 then 'adulte (25-55 ans)'
        when candidats.age_selon_nir > 55 then 'senior (+55 ans)'
    end as tranche_age,
    case
        when candidats.age_selon_nir > 16 and candidats.age_selon_nir < 25 then 'eligible CEJ'
        when candidats.age_selon_nir >= 57 then 'eligible CDI inclusion'
    end as eligibilite_dispositif,
    case
        when candidats.age_selon_nir > 16 and candidats.age_selon_nir < 25 then 'OUI'
        else 'NON'
    end as eligible_cej,
    case
        when candidats.age_selon_nir >= 57 then 'OUI'
        else 'NON'
    end as eligible_cdi_inclusion
from
    {{ ref('stg_candidats') }} as candidats
