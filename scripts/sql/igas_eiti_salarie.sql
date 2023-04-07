--drop table if exists igas_eiti_salarie;
create table igas_eiti_salarie as
with formations_par_contrat as (
    select
        contrat.contrat_id_pph as id_salarie,
        date_part('year', to_date(contrat.contrat_date_embauche, 'DD/MM/YYYY')) as annee_contrat,
        rnf.rnf_libelle_niveau_form_empl as niveau_formation,
        formations.formation_id_ctr,
        count(formations.*) as nb_formations,
        sum(formations.formation_duree_jours) as nb_jours_formation,
        sum(formations.formation_duree_heures) as nb_heures_formation,
        sum(formations.formation_duree_minutes) as nb_min_formation
    from
        "fluxIAE_Formations" formations
        left join "fluxIAE_ContratMission" contrat on formations.formation_id_ctr = contrat_id_ctr
        left join "fluxIAE_RefNiveauFormation" rnf on contrat.contrat_niveau_de_formation_code = rnf.rnf_code_niveau_form_empl
    where 
        contrat_mesure_disp_code = 'EITI_DC'
    group by
        contrat_id_pph,
        annee_contrat,
        formations.formation_id_ctr,
        rnf.rnf_libelle_niveau_form_empl
),
etp_par_salarie as (
    select distinct
        emi.emi_pph_id as id_salarie,
        af.af_id_annexe_financiere as id_annexe_financiere,
        structure.structure_denomination as denomination_structure,
        structure.nom_departement_structure as departement_structure,
        structure.nom_region_structure as region_structure,
        date_part('year',
            af.af_date_debut_effet_v2) as annee_af,
        sum(emi.emi_nb_heures_travail) as nombre_heures_travaillees,
        /*Nous calculons directement les ETPs réalisés pour éviter des problèmes de filtres/colonnes/etc sur metabase*/
        /* ETPs réalisés = Nbr heures travaillées / montant d'heures necessaires pour avoir 1 ETP */
        sum(emi.emi_nb_heures_travail / firmi.rmi_valeur) as nombre_etp_consommes_reels_annuels
    from
        "fluxIAE_EtatMensuelIndiv" as emi
    left join "fluxIAE_AnnexeFinanciere_v2" as af on emi.emi_afi_id = af.af_id_annexe_financiere
    left join "fluxIAE_RefMontantIae" firmi on af_mesure_dispositif_id = firmi.rme_id
    left join "fluxIAE_Structure_v2" as structure on af.af_id_structure = structure.structure_id_siae
where
    af.af_mesure_dispositif_code = 'EITI_DC'
    and firmi.rmi_libelle = 'Nombre d''heures annuelles théoriques pour un salarié à taux plein'
    and af.af_etat_annexe_financiere_code in('VALIDE',
        'PROVISOIRE',
        'CLOTURE')
    and af_mesure_dispositif_code not like '%FDI%'
group by
    id_salarie,
    annee_af,
    structure.nom_departement_structure,
    structure.nom_region_structure,
    af.af_id_annexe_financiere,
    structure.structure_denomination
)
select
    contrat.contrat_id_ctr as contrat_id,
    eps.*,
    salarie.salarie_rci_libelle as civilite,
    date_part('year', current_date) - salarie.salarie_annee_naissance as age,
    salarie.salarie_annee_naissance as annee_de_naissance,
    refmotifsort.rms_libelle as motif_sortie,
    categoriesort.rcs_libelle as categorie_sortie,
    contrat.contrat_date_embauche as date_embauche,
    formations.niveau_formation as niveau_formation_salarie,
    formations.nb_formations as nb_formations,
    formations.nb_jours_formation as nb_jours_formation,
    formations.nb_heures_formation as nb_heures_formation,
    formations.nb_min_formation as nb_min_formation,
    case when starts_with(contrat.contrat_salarie_rsa, 'OUI')  then
        'Oui'
    else
        'Non'
    end as brsa,
    case when contrat.contrat_salarie_rqth then
        'Oui'
    else
        'Non'
    end as rqth,
    case when contrat.contrat_salarie_aah then
        'Oui'
    else
        'Non'
    end as aah,
    case when contrat.contrat_salarie_oeth then
        'Oui'
    else
        'Non'
    end as oeth,
    case when contrat.contrat_salarie_ass then
        'Oui'
    else
        'Non'
    end as ass,
    case when contrat.contrat_salarie_is_zrr then
        'Oui'
    else
        'Non'
    end as zrr,
    case when contrat.contrat_salarie_aide_sociale then
        'Oui'
    else
        'Non'
    end as aide_soc,
    case when salarie.salarie_adr_qpv_type = 'QP' then
        'Oui'
    else
        'Non'
    end as qpv
from
    "fluxIAE_Salarie" salarie
    -- pour récupération du contrat réalisé pour un salarié
    -- une ligne par salarié et par contrat
    -- pour les EITI il y a un nb très négligeable de salariés ayant eu plusieurs contrats (5)
    left join "fluxIAE_ContratMission" contrat on contrat_id_pph = salarie_id
    left join etp_par_salarie eps on contrat_id_pph = eps.id_salarie 
    -- pour récupération du libellé du motif de sortie
    left join "fluxIAE_RefMotifSort" refmotifsort on rms_id = contrat_motif_sortie_id
    left join "fluxIAE_RefCategorieSort" as categoriesort on categoriesort.rcs_id = refmotifsort.rcs_id
    left join formations_par_contrat formations on formation_id_ctr = contrat_id_ctr and formations.annee_contrat = eps.annee_af
where
    contrat_mesure_disp_code = 'EITI_DC';