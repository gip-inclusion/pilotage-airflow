select
    {{ pilo_star(ref('sorties_v2'), relation_alias="sorties",
    except=["emi_nb_heures_travail", "emi_sme_version", "af_id_annexe_financiere", "emi_afi_id", "emi_sme_annee", "emi_sme_mois"]) }},
    disp.type_structure,
    disp.type_structure_emplois,
    nbr_h.total_heures_travaillees_derniere_af as total_heures_travaillees
from {{ ref('sorties_v2') }} as sorties
left join {{ ref('ref_mesure_dispositif_asp') }} as disp
    on sorties.af_mesure_dispositif_code = disp.af_mesure_dispositif_code
left join {{ ref('nombre_heures_travaillees_af') }} as nbr_h
    on nbr_h.emi_pph_id = sorties.emi_pph_id
where
    nbr_h.total_heures_travaillees_derniere_af >= 150
    and sorties.rcs_libelle != 'Retrait des sorties constatées'
    and disp.type_structure_emplois in ('AI', 'ETTI')
group by
    {{ pilo_star(ref('sorties_v2'), relation_alias="sorties",
    except=["emi_nb_heures_travail", "emi_sme_version", "af_id_annexe_financiere", "emi_afi_id", "emi_sme_annee", "emi_sme_mois"]) }},
    disp.type_structure,
    disp.type_structure_emplois,
    nbr_h.total_heures_travaillees_derniere_af
