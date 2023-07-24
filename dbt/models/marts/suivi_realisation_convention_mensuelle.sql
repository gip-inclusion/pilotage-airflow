select
    scec.id_annexe_financiere,
    scec.af_numero_convention,
    scec.af_numero_annexe_financiere,
    scec.af_etat_annexe_financiere_code,
    serps.date_saisie,
    serps.date_validation_declaration,
    scec.af_date_fin_effet_v2,
    scec.annee_af,
    scec.duree_annexe,
    scec."effectif_mensuel_conventionné",
    scec."effectif_annuel_conventionné",
    scec.type_structure,
    scec.structure_denomination,
    scec.commune_structure,
    scec.code_insee_structure,
    scec.siret_structure,
    scec.nom_departement_structure,
    scec.nom_region_structure,
    scec.code_departement_af,
    scec.nom_departement_af,
    scec.nom_region_af,
    case
        when serps.date_saisie is not null then 'Oui'
        else 'Non'
    end                                                    as saisie_effectuee,
    coalesce(serps.nombre_heures_travaillees, 0)           as nombre_heures_travaillees,
    coalesce(serps.nombre_etp_consommes_reels_annuels, 0)  as nombre_etp_consommes_reels_annuels,
    coalesce(serps.nombre_etp_consommes_reels_mensuels, 0) as nombre_etp_consommes_reels_mensuels
from
    {{ ref('suivi_complet_etps_conventionnes') }} as scec
left join {{ ref('suivi_etp_realises_par_structure') }} as serps
    on
        serps.id_annexe_financiere = scec.id_annexe_financiere
        and serps.annee = scec."année"
        and serps.mois = scec.month
