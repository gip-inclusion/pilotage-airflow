select
    c.id_structure,
    sum(c.nombre_de_candidatures) as somme_candidatures
from
    {{ ref('eph_nbr_candidatures') }} as c
group by
    c.id_structure
