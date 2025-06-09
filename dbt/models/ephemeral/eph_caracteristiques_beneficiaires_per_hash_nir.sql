select
    {{ pilo_star(ref('stg_caracteristiques_beneficiaires_ordonnees_per_hash_nir'), except=["rang"]) }}
from {{ ref('stg_caracteristiques_beneficiaires_ordonnees_per_hash_nir') }}
where rang = 1
