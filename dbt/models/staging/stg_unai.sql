select distinct
    ria."Réseau IAE" as reseau_unai,
    s.id             as id_structure,
    (ria."SIRET")    as siret
from {{ source('oneshot', 'reseau_iae_adherents') }} as ria
inner join {{ source('emplois', 'structures') }} as s
    on s.siret = ria."SIRET"
where ria."Réseau IAE" = 'Unai'
