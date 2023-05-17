select distinct
    ria."Réseau IAE" as reseau_coorace,
    s.id             as id_structure,
    (ria."SIRET")    as siret
from {{ source('oneshot', 'reseau_iae_adherents') }} as ria
inner join {{ source('emplois', 'structures') }} as s
    on s.siret = ria."SIRET"
where ria."Réseau IAE" = 'Coorace'
