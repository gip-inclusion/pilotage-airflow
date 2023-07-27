{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['emi_pph_id'], 'unique' : False},
      {'columns': ['af_numero_annexe_financiere'], 'unique' : False},
    ]
 ) }}

select distinct
    /* Here we need to exclude "af_motif_rejet_id" and "af_motif_rejet_code" because these
    two colums, for the same af, can contain two different informations.
    This will result in a duplication of all your data when you join the af table with another one */
    af.af_id_structure,
    af.af_mesure_dispositif_code,
    af.af_numero_annexe_financiere,
    af.af_id_annexe_financiere,
    af.denomination_structure,
    af.numero_departement_af,
    af.nom_departement_af,
    af.nom_region_af,
    emi.emi_pph_id,
    emi.emi_ctr_id,
    ctr.contrat_date_creation,
    ctr.contrat_date_embauche,
    ctr.contrat_date_sortie_definitive,
    ctr.contrat_date_fin_contrat,
    ctr.contrat_duree_contrat,
    ctr.contrat_salarie_rsa,
    emi.emi_afi_id,
    emi.emi_sme_mois,
    emi.emi_sme_annee,
    emi.emi_sme_version,
    emi.emi_nb_heures_travail,
    emi.emi_date_fin_reelle,
    rcs.rcs_libelle,
    rms.rms_libelle,
    extract(year from (ctr.contrat_date_embauche::DATE))          as annee_debut_contrat,
    extract(year from (ctr.contrat_date_sortie_definitive::DATE)) as annee_sortie_definitive,
    extract(year from (ctr.contrat_date_fin_contrat::DATE))       as annee_fin_contrat,
    date_trunc('year', date(extract(
        year from
        (ctr.contrat_date_sortie_definitive::DATE)
    ) || '-01-01'))                                               as debut_annee_fin_contrat
from
    {{ ref('fluxIAE_EtatMensuelIndiv_v2') }} as emi
left join {{ ref('fluxIAE_AnnexeFinanciere_v2') }} as af
    on af.af_id_annexe_financiere = emi.emi_afi_id
left join {{ ref('fluxIAE_ContratMission_v2') }} as ctr
    on ctr.contrat_id_ctr = emi.emi_ctr_id
left join {{ ref('fluxIAE_RefMotifSort_v2') }} as rms
    on emi.emi_motif_sortie_id = rms.rms_id
left join {{ ref('fluxIAE_RefCategorieSort_v2') }} as rcs
    on rms.rcs_id = rcs.rcs_id
where
    af.af_etat_annexe_financiere_code in ('VALIDE', 'PROVISOIRE', 'CLOTURE')
    and
    rms.rms_libelle is not null
    and emi.emi_sme_annee in (
        date_part('year', current_date),
        date_part('year', current_date) - 1,
        date_part('year', current_date) - 2
    )
