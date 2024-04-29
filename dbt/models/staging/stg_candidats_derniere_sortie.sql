select
    pass_iae,
    id_salarie_emplois,
    max(date_derniere_sortie)      as date_derniere_sortie,
    max(date_derniere_candidature) as date_derniere_candidature
from {{ ref('stg_candidats_asp_derniere_sortie') }}
where date_derniere_candidature > date_derniere_sortie
group by
    pass_iae,
    id_salarie_emplois
