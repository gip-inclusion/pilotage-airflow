with source as (
    select *
    from {{ ref('stg_emplois__candidats') }}
),

geographie as (
    select
        id_candidat,
        departement
    from {{ ref('int_emplois__candidats_geographie') }}
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
        candidats.id                                               as id_candidat,
        candidats.uid,
        candidats.hash_nir,
        candidats.sexe_selon_nir,
        candidats.age,
        candidats.code_commune_insee,
        geographie.departement,
        candidats.date_inscription,
        candidats.type_inscription,
        candidats.date_premiere_connexion,
        candidats.date_derniere_connexion,
        candidats.date_diagnostic,
        candidats.date_expiration_diagnostic,
        candidats.type_auteur_diagnostic,
        candidats.sous_type_auteur_diagnostic,
        candidats.id_auteur_diagnostic_prescripteur,
        candidats.id_auteur_diagnostic_employeur,
        candidats.total_criteres_niveau_1,
        candidats.total_criteres_niveau_2,
        candidats.date_mise_a_jour_metabase,
        candidats.pe_inscrit,
        candidats.pe_connect,
        candidats.actif,
        candidats.diagnostic_valide,
        candidats.critere_n1_beneficiaire_du_rsa,
        candidats.critere_n1_allocataire_ass,
        candidats.critere_n1_allocataire_aah,
        candidats.critere_n1_detld_plus_de_24_mois,
        candidats.critere_n2_niveau_d_etude_3_cap_bep_ou_infra,
        candidats.critere_n2_senior_plus_de_50_ans,
        candidats.critere_n2_jeune_moins_de_26_ans,
        candidats.critere_n2_sortant_de_l_ase,
        candidats.critere_n2_deld_12_a_24_mois,
        candidats.critere_n2_travailleur_handicape,
        candidats.critere_n2_parent_isole,
        candidats.critere_n2_personne_sans_hebergement_ou_hebergee_ou_ayant_u,
        candidats.critere_n2_refugie_statutaire_beneficiaire_d_une_protectio,
        candidats.critere_n2_resident_zrr,
        candidats.critere_n2_resident_qpv,
        candidats.critere_n2_sortant_de_detention_ou_personne_placee_sous_main,
        candidats.critere_n2_maitrise_de_la_langue_francaise_inferieure_au_ni,
        candidats.critere_n2_probleme_de_mobilite,
        candidats.injection_ai,
        candidats.tranche_age,
        candidats.has_crit_nv_1,
        candidats.has_crit_nv_2,
        candidats.has_crit_nv_2_etti_ai,

        pass_iae.date_debut_premier_pass_iae,
        pass_iae.date_debut_dernier_pass_iae,
        pass_iae.date_fin_dernier_pass_iae,
        candidats.has_crit_nv_1 or candidats.has_crit_nv_2         as is_eligible_pass_iae,
        candidats.has_crit_nv_1 or candidats.has_crit_nv_2_etti_ai as is_eligible_pass_iae_etti_ai,
        case
            when pass_iae.id_candidat is null then 'Sans PASS IAE'
            else pass_iae.statut_pass
        end                                                        as statut_dernier_pass_iae,
        coalesce(pass_iae.nombre_pass_iae, 0)                      as nombre_pass_iae
    from candidats_avec_criteres as candidats
    left join geographie
        on candidats.id = geographie.id_candidat
    left join dernier_pass_iae as pass_iae
        on candidats.id = pass_iae.id_candidat
)

select *
from final
