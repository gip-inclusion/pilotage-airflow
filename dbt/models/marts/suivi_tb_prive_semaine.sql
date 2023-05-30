select
    id_tb,
    nom_tb,
    semaine,
    -- nb total d'utilisateurs cette semaine
    count(id_utilisateur) as nb_utilisateurs,
    -- est-ce que l'utilisateur est revenu plus d'une fois cette semaine
    count(
        case
            when nb_visits > 1 then 1
        end
    )                     as nb_utilisateurs_plusieurs_visites
from {{ ref('suivi_utilisateurs_tb_prive_semaine') }}
group by
    id_tb,
    nom_tb,
    semaine
