select
    num_semaine,
    nom_tb,
    region,
    departement,
    type_utilisateur,
    type_organisation,
    -- calcul du nb d'utilisateurs revenus plusieurs fois sur une semaine
    count(
        case
            when nb_visites_sem > 1 then 1
        end
    ) as nb_utilisateurs_plusieurs_visites
from
    {{ ref('stg_nb_consult_utilisateur_semaine') }}
group by
    num_semaine,
    nom_tb,
    region,
    departement,
    type_utilisateur,
    type_organisation
