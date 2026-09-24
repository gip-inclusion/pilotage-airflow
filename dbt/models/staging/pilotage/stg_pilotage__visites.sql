select
    id_utilisateur,
    min(jour_visite) as premiere_visite,
    max(jour_visite) as derniere_visite
from {{ ref('suivi_utilisateurs_tb_prive_semaine') }}
group by id_utilisateur
