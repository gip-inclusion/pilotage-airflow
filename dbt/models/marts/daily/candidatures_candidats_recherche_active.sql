select
    cdd.id as id_candidature,
    cra.id as id_candidat
from {{ ref('stg_candidats_candidatures') }} as cdd
right join {{ ref('candidats_recherche_active') }} as cra
    on cra.id = cdd.id
-- certains candidats en recherche active ont reçu des candidatures avant 6 mois et elles ne nous intéressent pas ici
where cdd.date_candidature >= current_date - interval '6 months'
