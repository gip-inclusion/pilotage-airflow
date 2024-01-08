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
from suivi_utilisateurs_tb_prive_semaine
group by
    id_utilisateur,
    email_utilisateur,
    num_semaine,
    nom_tb,
    region,
    departement,
    type_utilisateur,
    type_organisation
