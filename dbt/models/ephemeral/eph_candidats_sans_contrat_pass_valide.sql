select
    hash_nir,
    code_departement_candidat,
    commune_candidat
from {{ ref('parcours_salarie') }} as parcours
where
    parcours.validite_dernier_pass = 'pass valide'
    and parcours.salarie_sortie_dernier_contrat = 'Oui'
