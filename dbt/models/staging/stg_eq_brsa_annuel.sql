select
    af.id_annexe_financiere,
    -- The number of bRSA per year is computed by the sum of the cofinanced amount in € / the duration of the af / 88% of the amount of the rsa
    sum(af.af_mt_cofinance / af.duree_annexe / rsa.rmi_valeur) as nb_brsa_cible_annuel
from {{ ref('stg_explose_par_mois_suivi_etp') }} as af
left join {{ ref ('stg_montants_rsa_verses') }} as rsa
    on
        date_trunc('month', af.date_annexe) = date_trunc('month', rsa.date_versement_rsa)
        and af.nom_departement_af = rsa.departement_pilotage
group by
    af.id_annexe_financiere
