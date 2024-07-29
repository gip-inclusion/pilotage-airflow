select
    ctr.id_recrutement,
    ctr.contrat_id_structure,
    ctr.contrat_id_pph,
    max(motif_sortie)                       as motif_sortie,
    array_agg(ctr.contrat_id_ctr)           as id_contrats,
    min(ctr.contrat_date_embauche)          as date_embauche_premier_contrat,
    max(ctr.contrat_date_fin_contrat)       as date_fin_dernier_contrat,
    max(ctr.contrat_date_sortie_definitive) as date_sortie_definitive_dernier_contrat,
    case
        when max(ctr.contrat_date_sortie_definitive) is not null then age(max(ctr.contrat_date_sortie_definitive), min(ctr.contrat_date_embauche))
        else age(max(ctr.contrat_date_fin_contrat), min(ctr.contrat_date_embauche))
    end                                     as duree_contrat_reelle_jours,
    case
        when max(ctr.contrat_date_sortie_definitive) is not null
            then
                (date_part('year', max(ctr.contrat_date_sortie_definitive)) - date_part('year', min(ctr.contrat_date_embauche))) * 12
                + (date_part('month', max(ctr.contrat_date_sortie_definitive)) - date_part('month', min(ctr.contrat_date_embauche)))
        else
            (date_part('year', max(ctr.contrat_date_fin_contrat)) - date_part('year', min(ctr.contrat_date_embauche))) * 12
            + (date_part('month', max(ctr.contrat_date_fin_contrat)) - date_part('month', min(ctr.contrat_date_embauche)))
    end                                     as duree_contrat_reelle_mois,
    sum(ctr.contrat_duree_contrat)          as duree_contrat_estimee,
    case
        when cgv_structs.siret is not null then 'Oui'
        else 'Non'
    end                                     as structure_convergence
from {{ ref('stg_contrats') }} as ctr
left join {{ ref("fluxIAE_Structure_v2") }} as structs
    on ctr.contrat_id_structure = structs.structure_id_siae
left join {{ ref("sirets_structures_convergence") }} as cgv_structs
    on structs.structure_siret_signature = cast(cgv_structs.siret as bigint)
where (contrat_mesure_disp_code = 'ACI_DC' or contrat_mesure_disp_code = 'ACI_MP') and contrat_nb_heures > 0
group by
    ctr.id_recrutement,
    ctr.contrat_id_structure,
    ctr.contrat_id_pph,
    cgv_structs.siret
