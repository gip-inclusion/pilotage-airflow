select
    hash_nir,
    (array_agg(ctr.nom_region_structure order by ctr.contrat_date_embauche desc))[1]           as region,
    (array_agg(ctr.nom_departement_structure order by ctr.contrat_date_embauche desc))[1]      as departement,
    (array_agg(ctr.code_dept_structure order by ctr.contrat_date_embauche desc))[1]            as code_departement,
    count(distinct ctr.contrat_id_structure)                                                   as nombre_structures,
    count(distinct ctr.contrat_mesure_disp_code)                                               as nombre_dispositifs_differents,
    count(distinct ctr.contrat_parent_id)                                                      as nombre_contrats_differents,
    array_agg(ctr.contrat_mesure_disp_code order by ctr.contrat_date_embauche)                 as liste_tous_dispositifs,
    array_agg(distinct ctr.contrat_mesure_disp_code)                                           as liste_dispositifs_differents,
    sum(ctr.duree_contrat_jours)                                                               as "durée_tous_contrats_jours",
    sum(ctr.duree_contrat_mois)                                                                as "durée_tous_contrats_mois",
    sum(ctr.duree_contrat_asp_mois)                                                            as "durée_tous_contrats_asp_mois",
    min(ctr.contrat_date_embauche)                                                             as date_embauche_premier_contrat,
    max(ctr.contrat_date_fin_contrat)                                                          as date_fin_dernier_contrat,
    (array_agg(ctr.contrat_date_sortie_definitive order by ctr.contrat_date_embauche desc))[1] as motif_sortie_dernier_contrat,
    (array_agg(ctr.contrat_date_sortie_definitive order by ctr.contrat_date_embauche desc))[1] as date_sortie_definitive_dernier_contrat,
    (array_agg(ctr.contrat_date_sortie_definitive order by ctr.contrat_date_embauche desc))[1] as salarie_sorti_dernier_contrat
from {{ ref("stg_contrats_parent") }} as ctr
left join {{ ref("stg_uniques_salarie_id") }} as salarie
    on ctr.contrat_id_pph = salarie.salarie_id
group by hash_nir
