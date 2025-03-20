select
    {{ pilo_star(ref('etat_mensuel_heures_travaillees_sorties'), relation_alias='ems') }},
    {{ pilo_star(ref('motif_sorties_salaries'), relation_alias='sorties') }},
    contrats.emi_pph_id as ctr_pph_id,
    contrats.emi_ctr_id as contrat_id,
    contrats.contrat_date_creation,
    contrats.contrat_date_embauche,
    contrats.contrat_date_sortie_definitive,
    contrats.contrat_date_fin_contrat,
    contrats.date_recrutement,
    contrats.date_sortie_definitive,
    contrats.contrat_duree_contrat,
    contrats.tranche_age,
    contrats.salarie_brsa,
    contrats.genre_salarie,
    contrats.annee_sortie_definitive,
    contrats.annee_fin_contrat,
    contrats.debut_annee_fin_contrat,
    contrats.duree_en_mois
from {{ ref("etat_mensuel_heures_travaillees_sorties") }} as ems
left join {{ ref("motif_sorties_salaries") }} as sorties
    on
        ems.emi_pph_id = sorties.sorties_pph_id
        and ems.emi_afi_id = sorties.sorties_afi_id
left join {{ ref("contrats_sorties") }} as contrats
    on
        ems.emi_pph_id = contrats.emi_pph_id
        and ems.emi_afi_id = contrats.emi_afi_id
        and ems.emi_ctr_id = contrats.emi_ctr_id
where
    ems.nombre_heures_travaillees >= 1
    and contrats.duree_en_mois > 2.65
    and ems.af_mesure_dispositif_code in ('ACI_DC', 'ACI_MP', 'EI_MP', 'EI_DC')
