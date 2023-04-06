drop table if exists prep;
create table prep as (
    select
        distinct(c.id),
        date_candidature,
        cd.date_début_contrat,
        hash_nir,
        sexe_selon_nir,
        age,
        cd.type_structure,
        cd.département_structure,
        cd.nom_département_structure,
        cd.région_structure,
        cd.injection_ai
    from
        candidats as c
    left join candidatures as cd
    on 
        c.id = cd.id_candidat
    where
        cd.injection_ai = 1
        and cd.type_structure = 'AI'
        and cd.date_candidature = '2021-11-30'
);
drop table if exists prep_asp;
create table prep_asp as (
    select 
        prep.id,
        date_candidature,
        date_début_contrat,
        p.date_début as date_debut_pass,
        p.date_fin as date_fin_pass,
        hash_nir,
        sexe_selon_nir,
        age,
        prep.type_structure,
        département_structure,
        nom_département_structure,
        région_structure,
        p.hash_numéro_pass_iae,
        prep.injection_ai
    from
        prep
    left join pass_agréments as p
    on
            prep.id = p.id_candidat
);
drop table if exists prep_rqth_sortie;
create table prep_rqth_sortie as (
    select
        id,
        fis.salarie_id,
        date_candidature,
        date_début_contrat,
        date_debut_pass,
        date_fin_pass,
        hash_nir,
        sexe_selon_nir,
        case
            when age is null then (
                2023 - fis.salarie_annee_naissance
            )
            when (
                2023 - fis.salarie_annee_naissance
            ) is null then age
            else 2023 - fis.salarie_annee_naissance
        end age_asp_ou_plateforme,
            case
                when age >= 50
                or (
                        2023 - fis.salarie_annee_naissance
                ) >= 50 then 'Oui'
                else 'Non'
            end senior,
                type_structure,
                département_structure,
                nom_département_structure,
                région_structure,
                prep_asp.hash_numéro_pass_iae,
                injection_ai
        from
                prep_asp
        left join "fluxIAE_Salarie" fis
    on
                prep_asp.hash_numéro_pass_iae = fis.hash_numéro_pass_iae
        where
            prep_asp.hash_numéro_pass_iae is not null
);
drop table if exists prep_sortie;
create table prep_sortie as (
    select 
        emi.emi_pph_id,
        contrat_id_pph,
        emi.emi_ctr_id,
        contrat_id_ctr,
        contrat_date_creation,
        contrat_date_modification,
        contrat_date_embauche,
        contrat_date_fin_contrat,
        contrat_duree_contrat,
        case 
            when ctr_mis.contrat_salarie_rqth = 'true' then 'Oui'
            when ctr_mis.contrat_salarie_rqth = 'false' then 'Non'
            else 'information non disponible'
        end rqth,
        sortie.rms_libelle as motif_sortie,
        categoriesort.rcs_libelle as categorie_sortie
    from
        "fluxIAE_EtatMensuelIndiv" as emi
    left join "fluxIAE_ContratMission" as ctr_mis 
        on
            emi.emi_ctr_id = ctr_mis.contrat_id_ctr
        and emi.emi_pph_id = ctr_mis.contrat_id_pph
    left join "fluxIAE_RefMotifSort" as sortie 
        on
            emi.Emi_motif_sortie_id = sortie.rms_id
    left join "fluxIAE_RefCategorieSort" as categoriesort
        on
            categoriesort.rcs_id = sortie.rcs_id
);
drop table if exists extraction_ai_unai;
create table extraction_ai_unai as (
    select
        distinct(id),
        date_début_contrat,
        contrat_date_fin_contrat,
        date_debut_pass,
        date_fin_pass,
        age_asp_ou_plateforme,
        senior,
        rqth,
        motif_sortie,
        categorie_sortie,
        type_structure,
        département_structure,
        nom_département_structure,
        région_structure
    from
        prep_rqth_sortie prs
    left join prep_sortie as ps
    on
        prs.salarie_id = ps.emi_pph_id
);