select
    etp_c.id_annexe_financiere,
    etp_c.af_numero_convention,
    etp_c.af_numero_annexe_financiere,
    etp_c.af_etat_annexe_financiere_code,
    etp_r.date_saisie,
    etp_r.date_validation_declaration,
    etp_c.af_date_fin_effet_v2,
    etp_c.annee_af,
    etp_c.duree_annexe,
    etp_c."effectif_mensuel_conventionné",
    etp_c."effectif_annuel_conventionné",
    etp_c.type_structure,
    etp_c.structure_denomination,
    etp_c.commune_structure,
    etp_c.code_insee_structure,
    etp_c.siret_structure,
    etp_c.nom_departement_structure,
    etp_c.nom_region_structure,
    etp_c.code_departement_af,
    etp_c.nom_departement_af,
    etp_c.nom_region_af,
    case
        when etp_r.date_saisie is not null then 'Oui'
        else 'Non'
    end                                                    as saisie_effectuee,
    coalesce(etp_r.nombre_heures_travaillees, 0)           as nombre_heures_travaillees,
    coalesce(etp_r.nombre_etp_consommes_reels_annuels, 0)  as nombre_etp_consommes_reels_annuels,
    coalesce(etp_r.nombre_etp_consommes_reels_mensuels, 0) as nombre_etp_consommes_reels_mensuels
from
    {{ ref('suivi_complet_etps_conventionnes') }} as etp_c
left join {{ ref('suivi_etp_realises_par_structure') }} as etp_r
    on
        etp_r.id_annexe_financiere = etp_c.id_annexe_financiere
        and etp_r.annee = etp_c."année"
        and etp_r.mois = etp_c.month
