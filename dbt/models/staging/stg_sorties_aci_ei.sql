/* For the ACI and EI the exit will be considered only
if the employee worked at least 3 months in the enteprise */

select
    {{ pilo_star(ref('sorties_v2'), relation_alias="sorties",
    except=["emi_nb_heures_travail", "emi_sme_version", "af_id_annexe_financiere", "emi_afi_id", "emi_sme_annee", "emi_sme_mois"]) }},
    nbr.nombre_mois_travailles,
    nbr_h.total_heures_travaillees_derniere_af,
    disp.type_structure,
    disp.type_structure_emplois,
    (extract(day from age(
        sorties.contrat_date_sortie_definitive::DATE, sorties.contrat_date_embauche::DATE
    )))
    as duree_contrat_regles_asp
from {{ ref('sorties_v2') }} as sorties
left join {{ ref('nombre_mois_travailles') }} as nbr
    on
        sorties.emi_pph_id = nbr.emi_pph_id
        and sorties.af_numero_annexe_financiere = nbr.af_numero_annexe_financiere
left join {{ ref('nombre_heures_travaillees_af') }} as nbr_h
    on
        sorties.emi_pph_id = nbr_h.emi_pph_id
        and sorties.af_numero_annexe_financiere = nbr_h.af_numero_annexe_financiere
left join {{ ref('ref_mesure_dispositif_asp') }} as disp
    on sorties.af_mesure_dispositif_code = disp.af_mesure_dispositif_code
where
    sorties.rcs_libelle != 'Retrait des sorties constatées' and disp.type_structure_emplois in ('ACI', 'EI')
    and sorties.duree_en_mois >= 3 and nbr_h.total_heures_travaillees_derniere_af > 0
group by
    {{ pilo_star(ref('sorties_v2'), relation_alias="sorties",
    except=["emi_nb_heures_travail", "emi_sme_version", "af_id_annexe_financiere", "emi_afi_id", "emi_sme_annee", "emi_sme_mois"]) }},
    nbr.nombre_mois_travailles,
    nbr_h.total_heures_travaillees_derniere_af,
    disp.type_structure,
    disp.type_structure_emplois
