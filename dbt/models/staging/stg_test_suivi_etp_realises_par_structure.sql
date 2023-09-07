select
    serv.id_annexe_financiere,
    serv.date_saisie,
    serv.emi_sme_mois,
    serv.emi_sme_annee,
    serv.date_validation_declaration,
    serv.af_numero_annexe_financiere,
    serv.code_departement_af,
    serv.nom_departement_af,
    serv.nom_region_af
from
    {{ ref('suivi_etp_realises_v2') }} as serv
group by
    serv.id_annexe_financiere,
    serv.date_saisie,
    serv.emi_sme_mois,
    serv.emi_sme_annee,
    serv.date_validation_declaration,
    serv.af_numero_annexe_financiere,
    serv.code_departement_af,
    serv.nom_departement_af,
    serv.nom_region_af
