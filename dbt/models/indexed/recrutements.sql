{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['contrat_id_ctr', 'contrat_id_pph'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select
    ctr.contrat_id_ctr,
    ctr.contrat_id_structure,
    ctr.contrat_id_pph,
    ctr.contrat_date_embauche,
    emi.emi_afi_id,
    emi.emi_sme_annee,
    emi.emi_sme_mois,
    af.af_numero_annexe_financiere,
    salarie.genre_salarie,
    salarie.tranche_age,
    salarie.qpv,
    salarie.zrr,
    rnf.rnf_libelle_niveau_form_empl                                      as niveau_formation_salarie,
    struct.structure_denomination,
    struct.structure_adresse_admin_commune,
    struct.structure_adresse_admin_code_insee,
    struct.nom_departement_structure,
    struct.nom_region_structure,
    struct.nom_epci_structure,
    af.nom_departement_af,
    af.nom_region_af,
    case
        when ctr.contrat_salarie_rqth then 'OUI'
        else 'NON'
    end                                                                   as salarie_rqth,
    case
        when ctr.contrat_salarie_rsa = 'OUI-M' then 'OUI'
        when ctr.contrat_salarie_rsa = 'OUI-NM' then 'OUI'
        else 'NON'
    end                                                                   as salarie_brsa,
    split_part(ctr.contrat_mesure_disp_code, '_', 1)                      as type_structure_entree,
    date_trunc('month', to_date(ctr.contrat_date_embauche, 'DD/MM/YYYY')) as mois_entree
from {{ ref('fluxIAE_ContratMission_v2') }} as ctr
left join {{ ref('fluxIAE_EtatMensuelIndiv_v2') }} as emi
    on ctr.contrat_id_pph = emi.emi_pph_id and ctr.contrat_id_ctr = emi.emi_ctr_id
left join {{ ref('fluxIAE_AnnexeFinanciere_v2') }} as af
    on emi.emi_afi_id = af.af_id_annexe_financiere
left join {{ ref('stg_salarie') }} as salarie
    on ctr.contrat_id_pph = salarie.salarie_id
left join {{ source('fluxIAE', 'fluxIAE_RefNiveauFormation') }} as rnf
    on ctr.contrat_niveau_de_formation_code = rnf.rnf_id
left join {{ ref('fluxIAE_Structure_v2') }} as struct
    on ctr.contrat_id_structure = struct.structure_id_siae
where
    ctr.contrat_type_contrat = 0
    and af.af_etat_annexe_financiere_code in (
        'VALIDE', 'PROVISOIRE', 'CLOTURE'
    )
