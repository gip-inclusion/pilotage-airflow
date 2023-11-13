select
    c."état",
    c.id_structure,
    sum(c.nombre_de_candidatures) as total_candidatures
from
    {{ ref('eph_nbr_candidatures') }} as c
group by
    c."état",
    c.id_structure
