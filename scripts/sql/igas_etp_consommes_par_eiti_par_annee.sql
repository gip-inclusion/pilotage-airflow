drop table if exists igas_etp_consommes_par_eiti_par_annee;
create table igas_etp_consommes_par_eiti_par_annee as (
    select
        structure.structure_id_siae as id_struct,
        date_part('year', af.af_date_debut_effet_v2) as annee_af,
        sum(emi.emi_nb_heures_travail) as nombre_heures_travaillees,
        sum(emi.emi_nb_heures_travail / firmi.rmi_valeur) as nombre_etp_consommes_reels_annuels
    from
        "fluxIAE_EtatMensuelIndiv" as emi
        left join "fluxIAE_AnnexeFinanciere_v2" as af on emi.emi_afi_id = af.af_id_annexe_financiere
        left join "fluxIAE_RefMontantIae" firmi on af_mesure_dispositif_id = firmi.rme_id
        left join "fluxIAE_Structure_v2" as structure on af.af_id_structure = structure.structure_id_siae
    where
        af.af_mesure_dispositif_code = 'EITI_DC'
        and firmi.rmi_libelle = 'Nombre d''heures annuelles théoriques pour un salarié à taux plein'
        and af.af_etat_annexe_financiere_code in('VALIDE', 'PROVISOIRE', 'CLOTURE')
        and af_mesure_dispositif_code not like '%FDI%'
    group by
        id_struct,
        annee_af
);
