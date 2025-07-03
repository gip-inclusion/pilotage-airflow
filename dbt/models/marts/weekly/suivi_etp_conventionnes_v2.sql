select
    {{ pilo_star(ref('stg_etp_conventionnes'), except=['af_mt_cofinance']) }},
    ("effectif_mensuel_conventionné" * af_montant_unitaire_annuel_valeur / 12 * duree_annexe)                                                                                  as "Montant_total_aide",
    coalesce(case when af_montant_total_annuel = 0 then 0 else af_mt_cofinance / af_montant_total_annuel end, 0)                                                               as part_conventionnement_cd,
    ("effectif_mensuel_conventionné" * coalesce(case when af_montant_total_annuel = 0 then 0 else af_mt_cofinance / af_montant_total_annuel end, 0) * duree_annexe / 12)       as etp_conventionnes_cd,
    -- ETP conventionnées Etat
    ("effectif_mensuel_conventionné" * (1 - coalesce(case when af_montant_total_annuel = 0 then 0 else af_mt_cofinance / af_montant_total_annuel end, 0)) * duree_annexe / 12) as etp_conventionnes_etat,
    coalesce(af_mt_cofinance, 0)                                                                                                                                               as af_mt_cofinance
    -- equivalent brsa removed from here, used in a smarter way in another table (suivi complet_etps_conventionnes)
from {{ ref('stg_etp_conventionnes') }}
