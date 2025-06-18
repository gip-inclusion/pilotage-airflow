select
    ctr.contrat_parent_id,
    ctr.contrat_id_pph,
    ctr.contrat_id_structure,
    ctr.contrat_mesure_disp_code,
    ctr.type_structure_emplois              as type_dispositif,
    ctr.structure_denomination_unique,
    max(ctr.nom_departement_structure)      as nom_departement_structure,
    max(ctr.nom_region_structure)           as nom_region_structure,
    max(ctr.code_dept_structure)            as code_dept_structure,
    min(ctr.contrat_date_embauche)          as contrat_date_embauche,
    max(ctr.contrat_date_sortie_definitive) as contrat_date_sortie_definitive,
    max(ctr.contrat_date_fin_contrat)       as contrat_date_fin_contrat,
    max(motif_sortie)                       as motif_sortie,
    max(categorie_sortie)                   as categorie_sortie,
    array_agg(
        ctr.contrat_id_ctr
        order by ctr.contrat_date_embauche
    )                                       as id_contrats,
    case
        when max(motif_sortie) is null then 'Non'
        else 'Oui'
    end                                     as salarie_sorti,
    case
        when max(ctr.contrat_date_sortie_definitive) is not null then {{ duration_in_days('min(ctr.contrat_date_embauche)', 'max(ctr.contrat_date_sortie_definitive)') }}
        else {{ duration_in_days('min(ctr.contrat_date_embauche)', 'CURRENT_DATE') }}
    end                                     as duree_contrat_jours,
    case
        when max(ctr.contrat_date_sortie_definitive) is not null then {{ duration_in_months('min(ctr.contrat_date_embauche)', 'max(ctr.contrat_date_sortie_definitive)') }}
        else {{ duration_in_months('min(ctr.contrat_date_embauche)', 'CURRENT_DATE') }}
    end                                     as duree_contrat_mois,
    sum(ctr.contrat_duree_contrat)          as duree_contrat_asp_mois,
    sum(ctr.contrat_nb_heures)              as contrat_nb_heures_travail
from {{ ref("stg_contrats") }} as ctr
where contrat_id_pph is not null
group by ctr.contrat_parent_id, contrat_id_pph, ctr.contrat_id_structure, ctr.structure_denomination_unique, ctr.contrat_mesure_disp_code, ctr.type_structure_emplois
