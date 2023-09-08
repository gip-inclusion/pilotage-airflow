select
    c.id,
    count(c.id) as total_candidats
from {{ ref('stg_candidats') }} as c
left join {{ source('emplois', 'candidatures') }} as cd
    on c.id = cd.id_candidat
where cd."état" = 'Candidature acceptée'
group by c.id
