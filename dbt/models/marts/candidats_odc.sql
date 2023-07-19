select distinct {{ pilo_star(source('emplois', 'candidats'), relation_alias="cdd") }}
from {{ source('emplois', 'candidats') }} as cdd
inner join
    {{ ref('candidatures_odc') }} as candidatures_odc
    on cdd.id = candidatures_odc.id_candidat
