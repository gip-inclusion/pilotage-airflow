select
    {{ pilo_star(ref('stg_consommation_etp'), relation_alias="etp") }},
    case
        /* On calcule la moyenne des etp consommés depuis le début de l'année et on la compare avec le nombre d'etp
                conventionnés */
        when
            etp.moyenne_nb_etp_mensuels_depuis_debut_annee
            < etp."effectif_mensuel_conventionné" then 'sous-consommation'
        when
            etp.moyenne_nb_etp_mensuels_depuis_debut_annee
            > etp."effectif_mensuel_conventionné" then 'sur-consommation'
        else 'conforme'
    end as consommation_etp
from
    {{ ref('stg_consommation_etp') }} as etp
