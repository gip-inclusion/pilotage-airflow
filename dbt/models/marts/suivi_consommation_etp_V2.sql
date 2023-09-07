select
    {{ pilo_star(ref('stg_consommation_etp'), relation_alias="etp") }},
    case
        /* On calcule la moyenne des etp consommés depuis le début de l'année et on la compare avec le nombre d'etp
                conventionnés */
        when
            trunc(cast(etp.moyenne_nb_etp_mensuels_depuis_debut_annee as numeric), 2)
            < trunc(cast(etp."effectif_mensuel_conventionné" as numeric), 2) then 'sous-consommation'
        when
            trunc(cast(etp.moyenne_nb_etp_mensuels_depuis_debut_annee as numeric), 2)
            > trunc(cast(etp."effectif_mensuel_conventionné" as numeric), 2) then 'sur-consommation'
        else 'conforme'
    end as consommation_etp,
    case
        when
            trunc(cast(etp.total_etp_annuels_realises as numeric), 2)
            < trunc(cast(etp."effectif_annuel_conventionné" as numeric), 2) then 'sous-consommation'
        when
            trunc(cast(etp.total_etp_annuels_realises as numeric), 2)
            > trunc(cast(etp."effectif_annuel_conventionné" as numeric), 2) then 'sur-consommation'
        else 'conforme'
    end as consommation_etp_annuels
from
    {{ ref('stg_consommation_etp') }} as etp
