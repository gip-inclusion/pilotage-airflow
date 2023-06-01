select
    nom_tb,
    semaine,
    num_semaine,
    -- nb total d'utilisateurs cette semaine
    count(id_utilisateur) as nb_utilisateurs,
    -- nb d'utilisateurs revenus au moins une fois cette semaine
    count(
        case
            when nb_visites > 1 then 1
        end
    )                     as nb_utilisateurs_plusieurs_visites
from {{ ref('suivi_utilisateurs_tb_prive_semaine') }}
group by
    nom_tb,
    semaine,
    num_semaine
