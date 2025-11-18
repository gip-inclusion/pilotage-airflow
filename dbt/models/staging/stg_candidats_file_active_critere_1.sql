select
    candidats.hash_nir,
    candidats."département"                                                                 as code_departement_candidat,
    (array_agg(salarie.salarie_commune order by salarie.salarie_date_modification desc))[1] as commune_candidat
from {{ ref('candidats_recherche_active') }} as candidats
left join {{ source("fluxIAE","fluxIAE_Salarie") }} as salarie
    on candidats.hash_nir = salarie.hash_nir
where
    candidats.hash_nir is not null
    and candidats.delai_premiere_candidature > 30
    and candidats.nb_candidatures_acceptees = 0
    and candidats.total_criteres_niveau_1 >= 1
group by
    candidats.hash_nir,
    candidats."département"
