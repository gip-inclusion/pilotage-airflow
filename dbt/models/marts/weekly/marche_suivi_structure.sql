select
    etp_r.identifiant_salarie,
    etp_c."effectif_annuel_conventionné",
    s.structure_siret_signature,
    s.structure_siret_actualise,
    s.structure_denomination,
    ctr.contrat_salarie_rqth,
    etp_r.id_annexe_financiere,
    etp_r.emi_sme_annee,
    salarie.salarie_annee_naissance,
    case
        when salarie.salarie_adr_qpv_type = 'QP' then 'QPV'
        when salarie.salarie_adr_qpv_type = 'NQP' then 'Non QPV'
        else 'Non renseigné'
    end as adresse_qpv,
    case
        when salarie.salarie_adr_is_zrr = 'TRUE' then 'ZRR'
        when salarie.salarie_adr_is_zrr = 'FALSE' then 'Non ZRR'
        else 'Non renseigné'
    end as adresse_zrr,
    case
        when salarie.salarie_rci_libelle = 'MME' then 'Femme'
        when salarie.salarie_rci_libelle = 'M.' then 'Homme'
        else 'Non renseigné'
    end as genre_salarie
from
    {{ ref('suivi_etp_realises_v2') }} as etp_r
left join {{ ref('fluxIAE_Salarie_v2') }} as salarie
    on etp_r.identifiant_salarie = salarie.salarie_id
left join {{ ref('suivi_etp_conventionnes_v2') }} as etp_c
    on etp_c.id_annexe_financiere = etp_r.id_annexe_financiere
left join {{ source('fluxIAE', 'fluxIAE_Structure') }} as s
    on etp_c.structure_id_siae = s.structure_id_siae
left join {{ ref('fluxIAE_ContratMission_v2') }} as ctr
    on etp_r.identifiant_salarie = ctr.contrat_id_pph
where etp_r.type_structure not in ('ACIPA_DC', 'EIPA_DC', 'FDI')
group by
    etp_r.identifiant_salarie,
    etp_c."effectif_annuel_conventionné",
    s.structure_siret_signature,
    s.structure_siret_actualise,
    s.structure_denomination,
    ctr.contrat_salarie_rqth,
    salarie.salarie_adr_qpv_type,
    salarie.salarie_adr_is_zrr,
    salarie.salarie_rci_libelle,
    etp_r.id_annexe_financiere,
    etp_r.emi_sme_annee,
    salarie.salarie_annee_naissance
