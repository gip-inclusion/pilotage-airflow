select distinct
    emi.emi_pph_id,
    emi.emi_afi_id,
    emi.emi_ctr_id,
    af.af_id_structure,
    af.af_mesure_dispositif_code,
    disp.type_structure,
    disp.type_structure_emplois,
    af.af_numero_annexe_financiere,
    af.af_id_annexe_financiere,
    af.denomination_structure,
    af.nom_departement_af,
    af.nom_region_af,
    emi.emi_sme_annee,
    sum(emi_nb_heures_travail) as nombre_heures_travaillees
from {{ ref('fluxIAE_EtatMensuelIndiv_v2') }} as emi
left join {{ ref('fluxIAE_AnnexeFinanciere_v2') }} as af
    on emi.emi_afi_id = af.af_id_annexe_financiere
left join {{ ref('ref_mesure_dispositif_asp') }} as disp
    on af.af_mesure_dispositif_code = disp.af_mesure_dispositif_code
where
    af.af_etat_annexe_financiere_code in ('VALIDE', 'PROVISOIRE', 'CLOTURE')
    and emi.emi_sme_annee >= 2021
    and af.af_mesure_dispositif_code not in ('ACIPA_DC', 'EIPA_DC')
group by
    emi.emi_pph_id,
    emi.emi_afi_id,
    emi.emi_ctr_id,
    af.af_id_structure,
    af.af_mesure_dispositif_code,
    disp.type_structure,
    disp.type_structure_emplois,
    af.af_numero_annexe_financiere,
    af.af_id_annexe_financiere,
    af.denomination_structure,
    af.nom_departement_af,
    af.nom_region_af,
    emi.emi_sme_annee
