select
    etp.identifiant_salarie,
    etp.id_annexe_financiere,
    etp.emi_sme_annee,
    etp.majoration_brsa,
    etp.type_structure,
    etp.type_structure_emplois,
    etp.structure_id_siae,
    etp.structure_denomination,
    etp.commune_structure,
    etp.code_insee_structure,
    etp.siret_structure,
    etp.epci_structure,
    etp.zone_emploi_structure,
    etp.nom_departement_af,
    etp.nom_region_af,
    etp.hash_nir,
    etp.genre_salarie,
    salarie.salarie_adr_zrr_classement as classement_zrr,
    salarie.tranche_age,
    case
        when salarie.salarie_adr_qpv_type = 'NQP' then 'Adresse non QPV'
        when salarie.salarie_adr_qpv_type = 'QP' then 'Adresse en QPV'
        else 'Statut QPV non renseigné'
    end                                as adresse_qpv,
    case
        when
            etp.salarie_brsa = 'OUI' then 'bRSA'
        else 'non bRSA'
    end                                as salarie_brsa,
    sum(etp.nombre_heures_travaillees) as nombre_heures_travaillees
from
    {{ ref('etp_par_salarie') }} as etp
left join {{ ref('fluxIAE_Salarie_v2') }} as salarie
    on etp.identifiant_salarie = salarie.salarie_id
group by
    etp.identifiant_salarie,
    etp.id_annexe_financiere,
    etp.emi_sme_annee,
    etp.majoration_brsa,
    etp.salarie_brsa,
    etp.type_structure,
    etp.type_structure_emplois,
    etp.structure_id_siae,
    etp.structure_denomination,
    etp.commune_structure,
    etp.code_insee_structure,
    etp.siret_structure,
    etp.epci_structure,
    etp.zone_emploi_structure,
    etp.nom_departement_af,
    etp.nom_region_af,
    etp.hash_nir,
    etp.genre_salarie,
    etp.genre_salarie,
    salarie.salarie_adr_qpv_type,
    salarie.tranche_age,
    salarie.salarie_adr_zrr_classement
