select
    etp.af_numero_annexe_financiere,
    /* Utilisation de l'ID de l'annexe financière -> ID unique contrairement à la convention */
    etp.annee_af,
    da.nombre_mois_saisis,
    etp."effectif_mensuel_conventionné",
    /* les deux conditions si dessous sont identiques
        sauf que pour l'une on considère les ETPs mensuels et l'autre les annuels */
    etp."effectif_annuel_conventionné",
    etp.duree_annexe,
    -- Ici on utilise le nombre de mois saisis éviter d'écrire une formule à rallonge
    etp.structure_id_siae,
    etp.type_structure,
    etp.structure_denomination,
    etp.structure_denomination_unique,
    etp.code_departement_af,
    etp.nom_departement_af,
    etp.nom_region_af,
    (etp.id_annexe_financiere),
    sum(etp_c.nombre_etp_consommes_reels_mensuels) as total_etp_mensuels_realises,
    sum(etp_c.nombre_etp_consommes_reels_annuels)  as total_etp_annuels_realises,
    case
        /* Sur les deux formules du dessous on sélectionne le dernier mois saisi pour avoir
        une moyenne mensuelle des ETPs consommés sur les années précédentes */
        -- Moyenne sur l'année N - 1
        when
            (
                max(etp.annee_af) = date_part(
                    'year',
                    current_date
                ) - 1
            )
            then
                (
                    sum(etp_c.nombre_etp_consommes_reels_mensuels) / da.nombre_mois_saisis
                )
        -- Moyenne sur l'année N - 2
        when
            (
                max(etp.annee_af) = date_part('year', current_date) - 2
            )
            then
                (
                    sum(etp_c.nombre_etp_consommes_reels_mensuels) / da.nombre_mois_saisis
                )
        -- Moyenne sur l'année actuelle
        /* Ici on lui demande de seulement prendre en compte les mois écoulés pour l'année en cours
        (donc en mars il divisera le total par 3) */
        else
            sum(etp_c.nombre_etp_consommes_reels_mensuels)
            filter
            (
                where
                etp.annee_af = (
                    date_part('year', current_date)
                )
            ) / da.nombre_mois_saisis
    end                                            as moyenne_nb_etp_mensuels_depuis_debut_annee,
    case
        -- Moyenne sur l'année N-1
        when
            (
                max(etp.annee_af) = date_part(
                    'year',
                    current_date
                ) - 1
            )
            then
                (
                    sum(etp_c.nombre_etp_consommes_reels_annuels) / da.nombre_mois_saisis
                )
        -- Moyenne sur l'année N-2
        when
            (
                max(etp.annee_af) = date_part('year', current_date) - 2
            )
            then
                (
                    sum(etp_c.nombre_etp_consommes_reels_annuels) / da.nombre_mois_saisis
                )
        -- Moyenne sur l'année actuelle
        else
            sum(etp_c.nombre_etp_consommes_reels_annuels)
            filter (
                where
                etp.annee_af = (
                    date_part('year', current_date)
                )
            ) / da.nombre_mois_saisis
    end                                            as moyenne_nb_etp_annuels_depuis_debut_annee
from
    {{ ref('suivi_etp_conventionnes_v2') }} as etp
left join
    {{ ref('suivi_etp_realises_v2') }} as etp_c
    on
        etp.id_annexe_financiere = etp_c.id_annexe_financiere
        and etp.af_numero_convention = etp_c.af_numero_convention
        and etp.af_numero_annexe_financiere = etp_c.af_numero_annexe_financiere
        and date_part('year', etp_c.date_saisie) = etp.annee_af
/* bien penser à joindre sur l'année pour éviter de se retrouver
avec années de conventionnement qui correspondent pas */
left join
    {{ ref('stg_duree_annexe') }} as da
    on
        da.id_annexe_financiere = etp.id_annexe_financiere
group by
    etp.duree_annexe,
    da.nombre_mois_saisis,
    etp.id_annexe_financiere,
    etp.af_numero_annexe_financiere,
    etp."effectif_mensuel_conventionné",
    etp."effectif_annuel_conventionné",
    etp.annee_af,
    etp.type_structure,
    etp.structure_id_siae,
    etp.structure_denomination,
    etp.structure_denomination_unique,
    etp.code_departement_af,
    etp.nom_departement_af,
    etp.nom_region_af
