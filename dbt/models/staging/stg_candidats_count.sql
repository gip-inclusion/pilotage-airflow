select
    c2.id,
    count(c2.id) as total_candidats
from {{ source('emplois', 'candidats') }} as c2
left join {{ source('emplois', 'candidatures') }} as cd2
    on c2.id = cd2.id_candidat
where cd2."état" = 'Candidature acceptée'
group by c2.id
