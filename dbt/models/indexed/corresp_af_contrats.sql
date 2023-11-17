{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['contrat_id_ctr', 'contrat_id_pph'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select distinct
    ctr.contrat_id_pph,
    ctr.contrat_id_ctr,
    af.af_numero_annexe_financiere,
    af.af_id_annexe_financiere,
    af.nom_departement_af,
    af.nom_region_af
from
    {{ ref('fluxIAE_EtatMensuelIndiv_v2') }} as emi
left join
    {{ ref('fluxIAE_ContratMission_v2') }} as ctr
    on emi.emi_ctr_id = ctr.contrat_id_ctr
left join
    {{ ref('fluxIAE_AnnexeFinanciere_v2') }} as af
    on emi.emi_afi_id = af.af_id_annexe_financiere
where
    af.af_etat_annexe_financiere_code in ('VALIDE', 'PROVISOIRE', 'CLOTURE')
