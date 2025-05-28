select
    etp.af_numero_annexe_financiere,
    etp.emi_sme_annee,
    etp.type_structure_emplois,
    etp.structure_denomination,
    etp.siret_structure,
    etp.nom_departement_af,
    etp.nom_region_af,
    count(distinct etp.hash_nir)                                                                                                               as nombre_de_beneficiaires,
    count(distinct case when etp.genre_salarie = 'Homme' then etp.hash_nir end)                                                                as dont_hommes,
    count(distinct case when etp.genre_salarie = 'Femme' then etp.hash_nir end)                                                                as dont_femmes,
    count(distinct case when ctr.contrat_salarie_rsa in ('OUI-M', 'OUI-NM') then etp.hash_nir end)                                             as dont_beneficiaires_du_rsa,
    count(distinct case when ctr.contrat_salarie_ass = true then etp.hash_nir end)                                                             as dont_beneficiaires_de_ass,
    count(distinct case when ctr.contrat_salarie_aah = true then etp.hash_nir end)                                                             as dont_beneficiaires_de_aah,
    count(distinct case when ctr.contrat_salarie_rqth = true then etp.hash_nir end)                                                            as dont_beneficiaires_de_rqth,
    count(distinct case when salarie.salarie_adr_is_zrr = true then etp.hash_nir end)                                                          as dont_beneficiaires_en_zrr,
    count(distinct case when salarie.salarie_adr_qpv_type = 'QP' then etp.hash_nir end)                                                        as dont_beneficiaires_en_qpv,
    count(distinct case when (etp.emi_sme_annee - salarie.salarie_annee_naissance) < 26 then etp.hash_nir end)                                 as dont_beneficiaires_jeunes,
    count(distinct case when (etp.emi_sme_annee - salarie.salarie_annee_naissance) >= 50 then etp.hash_nir end)                                as dont_beneficiaires_seniors,
    count(distinct case when ctr.duree_inscription_ft = 'DE 12 À 23 MOIS' then etp.hash_nir end)                                               as dont_beneficiaires_deld,
    count(distinct case when ctr.duree_inscription_ft = '24 MOIS ET PLUS' then etp.hash_nir end)                                               as dont_beneficiaires_detld,
    count(distinct case when ctr.niveau_de_formation in ('FORMATION DE NIVEAU CAP OU BEP', 'DIPLÔME OBTENU CAP OU BEP') then etp.hash_nir end) as dont_beneficiaires_formation_cap_bep,
    count(distinct case when ctr.niveau_de_formation = 'PERSONNES AVEC QUALIFICATIONS NON CERTIFIANTES' then etp.hash_nir end)                 as dont_beneficiaires_formation_non_certifiante,
    count(distinct case when ctr.niveau_de_formation in ('PAS DE FORMATION AU DELA DE LA SCOLARITE OBLIG.') then etp.hash_nir end)             as dont_beneficiaires_formation_inf_cap
from
    {{ ref('etp_par_salarie') }} as etp
left join {{ ref('fluxIAE_Salarie_v2') }} as salarie
    on etp.identifiant_salarie = salarie.salarie_id
left join {{ ref('fluxIAE_ContratMission_v2') }} as ctr
    on
        etp.identifiant_salarie = ctr.contrat_id_pph
        and etp.emi_ctr_id = ctr.contrat_id_ctr
group by
    etp.af_numero_annexe_financiere,
    etp.emi_sme_annee,
    etp.type_structure_emplois,
    etp.structure_denomination,
    etp.siret_structure,
    etp.nom_departement_af,
    etp.nom_region_af
having sum(etp.nombre_heures_travaillees) >= 1
