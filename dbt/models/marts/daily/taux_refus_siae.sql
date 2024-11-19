select
    {{ pilo_star(ref('taux_refus_structures')) }}
from
    {{ ref('taux_refus_structures') }}
where
    categorie_structure = 'IAE'
