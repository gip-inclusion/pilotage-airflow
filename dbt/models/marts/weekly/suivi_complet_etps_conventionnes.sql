select
    {{ pilo_star(ref('stg_explose_par_mois_suivi_etp'), relation_alias='af',
    except=["effectif_mensuel_conventionné",
        "effectif_annuel_conventionné",
        "nb_brsa_cible_mensuel",
        "nb_brsa_cible_annuel",
        "af_date_fin_effet_v2",
        "date_annexe"]) }},
    af.date_annexe::timestamp                               as af_date_fin_effet_v2,
    rsa.rmi_valeur                                          as montant_rsa,
    eq.nb_brsa_cible_annuel,
    -- The number of bRSA per month is computed by the cofinanced amount in € / the duration of the af / 88% of the amount of the rsa
    (af.af_mt_cofinance / af.duree_annexe / rsa.rmi_valeur) as nb_brsa_cible_mensuel,
    case
        when af.date_annexe >= af.af_date_debut_effet_v2 and af.date_annexe <= af.af_date_fin_effet_v2 then "effectif_mensuel_conventionné"
        else 0
    end                                                     as "effectif_mensuel_conventionné",
    case
        when af.date_annexe >= af.af_date_debut_effet_v2 and af.date_annexe <= af.af_date_fin_effet_v2 then "effectif_annuel_conventionné"
        else 0
    end                                                     as "effectif_annuel_conventionné",
    extract('year' from af.date_annexe)                     as "année",
    extract('month' from af.date_annexe)                    as mois
from {{ ref('stg_explose_par_mois_suivi_etp') }} as af
left join {{ ref ('stg_montants_rsa_verses') }} as rsa
    on
        date_trunc('month', af.date_annexe) = date_trunc('month', rsa.date_versement_rsa)
        and nom_departement_af = rsa.departement_pilotage
left join {{ ref('stg_eq_brsa_annuel') }} as eq
    on af.id_annexe_financiere = eq.id_annexe_financiere
group by
    {{ pilo_star(ref('stg_explose_par_mois_suivi_etp'), relation_alias='af',
    except=["effectif_mensuel_conventionné",
        "effectif_annuel_conventionné",
        "nb_brsa_cible_mensuel",
        "nb_brsa_cible_annuel",
        "af_date_fin_effet_v2",
        "date_annexe"]) }},
    af.date_annexe,
    af.af_date_fin_effet_v2,
    af."effectif_mensuel_conventionné",
    af."effectif_annuel_conventionné",
    "année",
    mois,
    eq.nb_brsa_cible_annuel,
    rsa.rmi_valeur
