select
    {{ pilo_star(ref('heures_ai_etti'), relation_alias='heures') }},
    contrats.emi_pph_id as ctr_pph_id,
    contrats.emi_ctr_id as contrat_id,
    contrats.contrat_date_creation,
    contrats.contrat_date_embauche,
    contrats.contrat_date_sortie_definitive,
    contrats.contrat_date_fin_contrat,
    contrats.date_recrutement,
    contrats.date_sortie_definitive,
    contrats.contrat_duree_contrat,
    contrats.contrat_salarie_rsa,
    contrats.annee_sortie_definitive,
    contrats.annee_fin_contrat,
    contrats.debut_annee_fin_contrat,
    contrats.duree_en_mois
from {{ ref("heures_ai_etti") }} as heures
left join {{ ref("contrats_sorties") }} as contrats
    on
        heures.emi_pph_id = contrats.emi_pph_id
        and heures.emi_afi_id = contrats.emi_afi_id
        and heures.emi_ctr_id = contrats.emi_ctr_id
        and heures.af_mesure_dispositif_code in ('AI_DC', 'ETTI_DC')
