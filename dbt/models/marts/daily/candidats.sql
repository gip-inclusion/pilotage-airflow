select
    {{ pilo_star(ref('stg_candidats')) }},
    cdd.date_derniere_candidature,
    cdd.date_premiere_candidature,
    case
        when candidats.date_diagnostic > current_date - interval '6 months' then 1
        else 0
    end as diagnostic_valide,
    case
        when candidats.age_selon_nir <= 25 then 'Jeune (- de 26 ans)'
        when candidats.age_selon_nir > 25 and candidats.age_selon_nir <= 54 then 'Adulte (26-54 ans)'
        when candidats.age_selon_nir >= 55 then 'Senior (55 ans et +)'
        else 'Non renseigné'
    end as tranche_age,
    case
        when candidats.age_selon_nir > 16 and candidats.age_selon_nir <= 25 then 'Éligible CEJ'
        when candidats.age_selon_nir > 16 and candidats.age_selon_nir <= 30 then 'Éligible CEJ si RQTH'
        when candidats.age_selon_nir >= 57 then 'Éligible CDI inclusion'
        else 'Non renseigné'
    end as eligibilite_dispositif,
    case
        when candidats.age_selon_nir > 16 and candidats.age_selon_nir <= 25 then 'OUI'
        else 'NON'
    end as eligible_cej,
    case
        when candidats.age_selon_nir >= 57 then 'OUI'
        else 'NON'
    end as eligible_cdi_inclusion
from
    {{ ref('stg_candidats') }} as candidats
left join
    {{ ref('stg_candidatures_candidats') }} as cdd
    on cdd.id_candidat = candidats.id
