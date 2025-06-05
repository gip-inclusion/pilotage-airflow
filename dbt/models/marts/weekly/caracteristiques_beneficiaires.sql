select
    etp.af_numero_annexe_financiere,
    etp.id_annexe_financiere,
    etp.identifiant_salarie,
    etp.hash_nir,
    ctr.contrat_id_ctr,
    ctr.contrat_date_embauche,
    etp.emi_sme_annee,
    etp.type_structure_emplois,
    etp.structure_denomination,
    etp.siret_structure,
    etp.nom_departement_af,
    etp.nom_region_af,
    etp.genre_salarie,
    ctr.contrat_salarie_ass,
    ctr.contrat_salarie_aah,
    ctr.contrat_salarie_rqth,
    salarie.salarie_adr_is_zrr,
    ctr.duree_inscription_ft,
    --here 'DE 12 À 23 MOIS' = demandeur emploi longue durée (DELD); '24 MOIS ET PLUS' = demandeur emploi tres longue durées (DETLD)
    to_date(ctr.contrat_date_sortie_definitive, 'DD/MM/YYYY') as contrat_date_sortie_definitive,
    etp.id_annexe_financiere || '-' || ctr.contrat_id_ctr     as id_af_contrat, --this column is created to be the primary key
    case
        when ctr.contrat_salarie_rsa in ('OUI-M', 'OUI-NM') then 'bRSA'
        else 'non bRSA'
    end                                                       as beneficiaires_du_rsa,
    case
        when salarie.salarie_adr_qpv_type = 'QP' then 'salarie QPV'
        else 'salarie non QPV'
    end                                                       as salarie_qpv,

    case
        when (etp.emi_sme_annee - salarie.salarie_annee_naissance) < 26 then 'salarié jeune'
        when (etp.emi_sme_annee - salarie.salarie_annee_naissance) >= 50 then 'salarié senior'
        else 'salarie adulte'
    end                                                       as tranche_age_beneficiaires,
    case
        when ctr.niveau_de_formation in ('FORMATION DE NIVEAU CAP OU BEP', 'DIPLÔME OBTENU CAP OU BEP') then 'Formation de niveau CAP ou BEP'
        when ctr.niveau_de_formation = 'PERSONNES AVEC QUALIFICATIONS NON CERTIFIANTES' then 'Formation non certifiante'
        when ctr.niveau_de_formation in ('PAS DE FORMATION AU DELA DE LA SCOLARITE OBLIG.') then 'Formation inférieure au CAP'
        else ctr.niveau_de_formation
    end                                                       as niveau_de_formation
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
    etp.id_annexe_financiere,
    etp.identifiant_salarie,
    ctr.contrat_id_ctr,
    ctr.contrat_date_embauche,
    contrat_date_sortie_definitive,
    etp.type_structure_emplois,
    etp.structure_denomination,
    etp.siret_structure,
    etp.nom_departement_af,
    etp.nom_region_af,
    etp.hash_nir,
    etp.genre_salarie,
    beneficiaires_du_rsa,
    ctr.contrat_salarie_ass,
    ctr.contrat_salarie_aah,
    ctr.contrat_salarie_rqth,
    salarie.salarie_adr_is_zrr,
    salarie_qpv,
    tranche_age_beneficiaires,
    niveau_de_formation,
    ctr.duree_inscription_ft
having sum(etp.nombre_heures_travaillees) >= 1
