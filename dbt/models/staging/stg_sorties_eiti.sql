select
    {{ pilo_star(ref('sorties_v2'), relation_alias="sorties",
    except=["emi_nb_heures_travail", "emi_sme_version", "af_id_annexe_financiere", "emi_afi_id", "emi_sme_annee", "emi_sme_mois"]) }},
    disp.type_structure,
    disp.type_structure_emplois
from {{ ref('sorties_v2') }} as sorties
left join {{ ref('ref_mesure_dispositif_asp') }} as disp
    on sorties.af_mesure_dispositif_code = disp.af_mesure_dispositif_code
where
    sorties.rcs_libelle != 'Retrait des sorties constatées' and disp.type_structure_emplois in ('EITI')
group by
    {{ pilo_star(ref('sorties_v2'), relation_alias="sorties",
    except=["emi_nb_heures_travail", "emi_sme_version", "af_id_annexe_financiere", "emi_afi_id", "emi_sme_annee", "emi_sme_mois"]) }},
    disp.type_structure,
    disp.type_structure_emplois
