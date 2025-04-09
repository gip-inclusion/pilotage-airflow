select
    {{ pilo_star(ref('stg_explose_par_mois_suivi_etp'), except=["effectif_mensuel_conventionné","effectif_annuel_conventionné","nb_brsa_cible_mensuel","nb_brsa_cible_annuel","af_date_fin_effet_v2","date_annexe"]) }},
    date_annexe::timestamp            as af_date_fin_effet_v2,
    case
        when date_annexe >= af_date_debut_effet_v2 and date_annexe <= af_date_fin_effet_v2 then "effectif_mensuel_conventionné"
        else 0
    end                               as "effectif_mensuel_conventionné",
    case
        when date_annexe >= af_date_debut_effet_v2 and date_annexe <= af_date_fin_effet_v2 then "effectif_annuel_conventionné"
        else 0
    end                               as "effectif_annuel_conventionné",
    case
        when date_annexe >= af_date_debut_effet_v2 and date_annexe <= af_date_fin_effet_v2 then nb_brsa_cible_mensuel
        else 0
    end                               as nb_brsa_cible_mensuel,
    case
        when date_annexe >= af_date_debut_effet_v2 and date_annexe <= af_date_fin_effet_v2 then nb_brsa_cible_annuel
        else 0
    end                               as nb_brsa_cible_annuel,
    extract('year' from date_annexe)  as "année",
    extract('month' from date_annexe) as mois
from {{ ref('stg_explose_par_mois_suivi_etp') }}
