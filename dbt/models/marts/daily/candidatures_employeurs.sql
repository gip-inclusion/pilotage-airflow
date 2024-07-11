select
    id_candidat,
    id_structure,
    nom_structure,
    count(id) as nombre_candidatures
from
    {{ ref('candidatures_echelle_locale') }}
group by
    id_candidat,
    id_structure,
    nom_structure
having count(id) > 1
