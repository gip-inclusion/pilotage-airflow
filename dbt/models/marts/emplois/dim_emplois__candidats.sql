with source as (
    select *
    from {{ ref('stg_emplois__candidats') }}
),

candidats_avec_criteres as (
    select
        *,
        case
            when age < 0 or age is null then 'Non renseigné'
            when age < 26 then 'Moins de 26 ans'
            when age < 55 then '26–54 ans'
            else '55 ans et plus'
        end                                           as tranche_age,
        coalesce(total_criteres_niveau_1 >= 1, false) as has_crit_nv_1,
        coalesce(total_criteres_niveau_2 >= 3, false) as has_crit_nv_2,
        coalesce(total_criteres_niveau_2 >= 2, false) as has_crit_nv_2_etti_ai
    from source
),

pass_iae_classes as (
    select
        id_candidat,
        date_debut,
        date_fin,
        statut_pass,
        count(*) over (partition by id_candidat)        as nombre_pass_iae,
        min(date_debut) over (partition by id_candidat) as date_debut_premier_pass_iae,
        row_number() over (
            partition by id_candidat
            order by
                date_debut desc nulls last,
                coalesce(is_duree_indeterminee, false) desc,
                date_fin desc nulls last,
                id_pass_iae desc
        )                                               as rang
    from {{ ref('fct_emplois__pass_iae') }}
),

dernier_pass_iae as (
    select
        id_candidat,
        nombre_pass_iae,
        date_debut_premier_pass_iae,
        date_debut as date_debut_dernier_pass_iae,
        date_fin   as date_fin_dernier_pass_iae,
        statut_pass
    from pass_iae_classes
    where rang = 1
),

final as (
    select
        id                                     as id_candidat,
        uid,
        hash_nir,
        sexe_selon_nir,
        age,
        date_inscription,
        type_inscription,
        date_premiere_connexion,
        date_derniere_connexion,
        date_diagnostic,
        date_expiration_diagnostic,
        type_auteur_diagnostic,
        sous_type_auteur_diagnostic,
        id_auteur_diagnostic_prescripteur,
        id_auteur_diagnostic_employeur,
        total_criteres_niveau_1,
        total_criteres_niveau_2,
        date_mise_a_jour_metabase,
        pe_inscrit,
        pe_connect,
        actif,
        diagnostic_valide,
        critere_n1_beneficiaire_du_rsa,
        critere_n1_allocataire_ass,
        critere_n1_allocataire_aah,
        critere_n1_detld_plus_de_24_mois,
        critere_n2_niveau_d_etude_3_cap_bep_ou_infra,
        critere_n2_senior_plus_de_50_ans,
        critere_n2_jeune_moins_de_26_ans,
        critere_n2_sortant_de_l_ase,
        critere_n2_deld_12_a_24_mois,
        critere_n2_travailleur_handicape,
        critere_n2_parent_isole,
        critere_n2_personne_sans_hebergement_ou_hebergee_ou_ayant_u,
        critere_n2_refugie_statutaire_beneficiaire_d_une_protectio,
        critere_n2_resident_zrr,
        critere_n2_resident_qpv,
        critere_n2_sortant_de_detention_ou_personne_placee_sous_main,
        critere_n2_maitrise_de_la_langue_francaise_inferieure_au_ni,
        critere_n2_probleme_de_mobilite,
        injection_ai,
        tranche_age,
        has_crit_nv_1,
        has_crit_nv_2,
        has_crit_nv_2_etti_ai,

        pass_iae.date_debut_premier_pass_iae,
        pass_iae.date_debut_dernier_pass_iae,
        pass_iae.date_fin_dernier_pass_iae,
        has_crit_nv_1 or has_crit_nv_2         as is_eligible_pass_iae,
        has_crit_nv_1 or has_crit_nv_2_etti_ai as is_eligible_pass_iae_etti_ai,
        case
            when pass_iae.id_candidat is null then 'Sans PASS IAE'
            else pass_iae.statut_pass
        end                                    as statut_dernier_pass_iae,
        coalesce(pass_iae.nombre_pass_iae, 0)  as nombre_pass_iae
    from candidats_avec_criteres as candidats
    left join dernier_pass_iae as pass_iae
        on candidats.id = pass_iae.id_candidat
)

select *
from final
