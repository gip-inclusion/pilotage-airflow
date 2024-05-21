select
    rec.id_premier_contrat           as identifiant_contrat,
    rec.id_derniere_reconduction     as identifiant_derniere_reconduction_contrat,
    ctr.contrat_id_structure,
    rec.contrat_id_pph               as identifiant_salarie,
    ctr.contrat_date_embauche,
    salarie.genre_salarie,
    salarie.tranche_age,
    salarie.qpv,
    salarie.zrr,
    rnf.rnf_libelle_niveau_form_empl as niveau_formation_salarie,
    struct.structure_denomination,
    struct.structure_adresse_admin_commune,
    struct.structure_adresse_admin_code_insee,
    struct.nom_departement_structure,
    struct.nom_region_structure,
    struct.nom_epci_structure,
    ref_disp.type_structure_emplois  as type_siae,
    af_ctr.af_numero_annexe_financiere,
    af_ctr.af_id_annexe_financiere,
    af_ctr.nom_departement_af,
    af_ctr.nom_region_af,
    rec.nb_reconductions,
    rec.date_recrutement,
    rec.date_fin_recrutement,
    emi.date_recrutement_reelle,
    case
        when ctr.contrat_salarie_rqth then 'OUI'
        else 'NON'
    end                              as rqth,
    case
        when ctr.contrat_salarie_rsa = 'OUI-M' then 'OUI'
        when ctr.contrat_salarie_rsa = 'OUI-NM' then 'OUI'
        else 'NON'
    end                              as brsa
from {{ ref('stg_recrutements') }} as rec
inner join {{ ref('fluxIAE_ContratMission_v2') }} as ctr
    on rec.id_premier_contrat = ctr.contrat_id_ctr
left join {{ ref('corresp_af_contrats') }} as af_ctr
    on ctr.contrat_id_ctr = af_ctr.contrat_id_ctr and ctr.contrat_id_pph = af_ctr.contrat_id_pph
left join {{ ref('stg_salarie') }} as salarie
    on ctr.contrat_id_pph = salarie.salarie_id
left join {{ source('fluxIAE', 'fluxIAE_RefNiveauFormation') }} as rnf
    on ctr.contrat_niveau_de_formation_code = rnf.rnf_id
left join {{ ref('fluxIAE_Structure_v2') }} as struct
    on ctr.contrat_id_structure = struct.structure_id_siae
left join {{ ref('ref_mesure_dispositif_asp') }} as ref_disp
    on ctr.contrat_mesure_disp_code = ref_disp.af_mesure_dispositif_code
inner join {{ ref('stg_emi_date_recrutement') }} as emi
    on rec.id_premier_contrat = emi.emi_ctr_id
