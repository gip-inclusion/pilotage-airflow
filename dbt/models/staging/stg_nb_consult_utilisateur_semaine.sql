select
    num_semaine,
    email_utilisateur,
    id_utilisateur,
    nom_tb,
    region,
    departement,
    type_utilisateur,
    type_organisation,
    -- nb de fois ou l'utilisateur est revenu cette semaine
    count(*) as nb_visites_sem
from {{ ref('suivi_utilisateurs_tb_prive_semaine') }}
group by
    num_semaine,
    email_utilisateur,
    id_utilisateur,
    nom_tb,
    region,
    departement,
    type_utilisateur,
    type_organisation
