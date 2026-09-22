select
    extract(year from derniere_reponse_barometre)  as annee,
    extract(month from derniere_reponse_barometre) as mois,
    count(*)                                       as total_answers
from
    {{ ref('stg_starmetric') }}
group by
    annee, mois
