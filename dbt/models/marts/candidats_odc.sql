select {{ pilo_star(source('emplois', 'candidats')) }}
from {{ source('emplois', 'candidats') }}
where id in (select id_candidat from {{ ref('candidatures_odc') }})
