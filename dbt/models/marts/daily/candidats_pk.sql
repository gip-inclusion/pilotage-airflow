select
    salarie_asp."hash_numéro_pass_iae" as "hash_numéro_pass_iae_asp",
    pass."hash_numéro_pass_iae"        as "hash_numéro_pass_iae_emplois",
    salarie_asp.salarie_id             as id_salarie_asp,
    candidat_emploi.id                 as id_candidat_emplois,
    candidat_emploi.hash_nir           as hash_nir
from {{ source('fluxIAE', 'fluxIAE_Salarie') }} as salarie_asp
left join {{ source('emplois', 'pass_agréments') }} as pass
    on salarie_asp."hash_numéro_pass_iae" = pass."hash_numéro_pass_iae"
left join {{ ref('candidats') }} as candidat_emploi
    on candidat_emploi.id = pass.id_candidat
