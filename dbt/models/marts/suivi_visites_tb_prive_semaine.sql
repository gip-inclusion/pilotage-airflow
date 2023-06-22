select
    nom_tb,
    semaine,
    num_semaine,
    -- mail des utilisateurs venus cette semaine
    array_agg(c1_users.email) as liste_utilisateurs,
    -- nb total d'utilisateurs cette semaine
    count(id_utilisateur)     as nb_utilisateurs,
    -- nb d'utilisateurs revenus au moins une fois cette semaine
    count(
        case
            when nb_visites > 1 then 1
        end
    )                         as nb_utilisateurs_plusieurs_visites
from {{ ref('suivi_utilisateurs_tb_prive_semaine') }} as visits
left join {{ source('emplois', 'utilisateurs') }} as c1_users
    on c1_users.id = cast(visits.id_utilisateur as INTEGER)
group by
    num_semaine,
    nom_tb,
    semaine
