select
    {{ pilo_star(ref('stg_candidats_derniere_sortie'), relation_alias = 'last_s') }},
    asp_s.cause_sortie,
    asp_s.motif_sortie
from {{ ref('stg_candidats_derniere_sortie') }} as last_s
left join {{ ref('stg_candidats_asp_derniere_sortie') }} as asp_s
    on
        last_s.id_salarie_emplois = asp_s.id_salarie_emplois
        and last_s.date_derniere_sortie = asp_s.date_derniere_sortie
        and last_s.date_derniere_candidature = asp_s.date_derniere_candidature
where last_s.date_derniere_candidature > last_s.date_derniere_sortie
