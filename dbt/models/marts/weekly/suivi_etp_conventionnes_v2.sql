select
    {{ pilo_star(ref('stg_etp_conventionnes'), except=['af_mt_cofinance']) }},
    ("effectif_mensuel_conventionné" * af_montant_unitaire_annuel_valeur / 12 * duree_annexe)                                                                                  as "Montant_total_aide",
    coalesce(case when af_montant_total_annuel = 0 then 0 else af_mt_cofinance / af_montant_total_annuel end, 0)                                                               as part_conventionnement_cd,
    ("effectif_mensuel_conventionné" * coalesce(case when af_montant_total_annuel = 0 then 0 else af_mt_cofinance / af_montant_total_annuel end, 0) * duree_annexe / 12)       as etp_conventionnes_cd,
    -- ETP conventionnées Etat
    ("effectif_mensuel_conventionné" * (1 - coalesce(case when af_montant_total_annuel = 0 then 0 else af_mt_cofinance / af_montant_total_annuel end, 0)) * duree_annexe / 12) as etp_conventionnes_etat,
    -- Nombre de brsa pouvant être subventionnés avec le montant cofinancé (par mois)
    (af_mt_cofinance / duree_annexe / (0.88 * montant_rsa))                                                                                                                    as nb_brsa_cible_mensuel,
    -- idem par an
    (af_mt_cofinance / (0.88 * montant_rsa))                                                                                                                                   as nb_brsa_cible_annuel,
    -- remplissage des null avec des 0 afin de ne pas casser les scripts
    -- dependants de cette table lors du début d'une nouvelle année
    coalesce(af_mt_cofinance, 0)                                                                                                                                               as af_mt_cofinance
from {{ ref('stg_etp_conventionnes') }}
