select distinct
    territoire_id,
    territoire_libelle,
    departement_id,
    nom_departement,
    nom_region
from {{ ref('ref_clpe_ft') }}
-- les CLPE interdépartementaux ont des communes dans plusieurs départements : on garde le département porteur
where territoire_id like concat('CLPE', departement_id, '%')
