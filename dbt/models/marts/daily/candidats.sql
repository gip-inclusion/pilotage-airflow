select
    {{ pilo_star(ref('stg_candidats')) }},
    case
        when age_selon_nir <= 25 then 'jeune (- de 26 ans)'
        when age_selon_nir > 25 and age_selon_nir <= 54 then 'adulte (26-54 ans)'
        when age_selon_nir >= 55 then 'senior (55 ans et +)'
        else 'non communiqué'
    end as tranche_age,
    case
        when age_selon_nir > 16 and age_selon_nir <= 25 then 'eligible CEJ'
        when age_selon_nir > 16 and age_selon_nir <= 30 then 'eligible CEJ si RQTH'
        when age_selon_nir >= 57 then 'eligible CDI inclusion'
        else 'non communiqué'
    end as eligibilite_dispositif,
    case
        when age_selon_nir > 16 and age_selon_nir <= 25 then 'OUI'
        else 'NON'
    end as eligible_cej,
    case
        when age_selon_nir >= 57 then 'OUI'
        else 'NON'
    end as eligible_cdi_inclusion
from
    {{ ref('stg_candidats') }}
