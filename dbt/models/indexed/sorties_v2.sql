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
    case
        when ctr.contrat_salarie_rsa = 'OUI-M' then 'RSA majoré'
        when ctr.contrat_salarie_rsa = 'OUI-NM' then 'RSA non majoré'
        else 'Non bénéficiaire du RSA'
    end                                                                          as majoration_brsa,
    case
        when ctr.contrat_salarie_rsa = 'OUI-M' then 'OUI'
        when ctr.contrat_salarie_rsa = 'OUI-NM' then 'OUI'
        else 'NON'
    end                                                                          as salarie_brsa,
    case
        when salarie.salarie_rci_libelle = 'MME' then 'Femme'
        when salarie.salarie_rci_libelle = 'M.' then 'Homme'
        else 'Non renseigné'
    end                                                                          as genre_salarie,
    extract(year from ctr.contrat_date_embauche)                                 as annee_debut_contrat,
    extract(year from to_date(ctr.contrat_date_sortie_definitive, 'DD/MM/YYYY')) as annee_sortie_definitive,
    extract(year from ctr.contrat_date_fin_contrat)                              as annee_fin_contrat,
    date_trunc('year', date(extract(
        year from
        to_date(ctr.contrat_date_sortie_definitive, 'DD/MM/YYYY')
    ) || '-01-01'))                                                              as debut_annee_fin_contrat
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
left join {{ ref('fluxIAE_Salarie_v2') }} as salarie
    on salarie.salarie_id = emi.emi_pph_id
where
    af.af_etat_annexe_financiere_code in ('VALIDE', 'PROVISOIRE', 'CLOTURE')
    and
    rms.rms_libelle is not null
    /* Gardons un historique à partir de 2022.
    Néamoins, pour que les sorties soient de 2022 soient calculées correctement
    il faut prendre en compte l'année 2021 */
    and emi.emi_sme_annee >= 2021
