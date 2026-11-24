with candidatures as (
    select *
    from {{ ref('fct_emplois__candidatures') }}
),

communes as (
    select *
    from {{ ref('dim_commune') }}
),

etablissements as (
    select
        etab.*,
        communes.nom_commune,
        communes.code_departement_insee,
        communes.nom_departement,
        communes.nom_departement_complet,
        communes.code_region_insee,
        communes.nom_region,
        communes.code_ft_clpe,
        communes.nom_clpe
    from {{ ref('dim_emplois__etablissements') }} as etab
    left join communes
        on etab.code_commune = communes.code_commune_insee
)

select
    candidatures.id_candidature,
    candidatures.id_candidat,
    candidatures.origine_candidature,
    candidatures.origine_detaillee,
    candidatures.type_de_candidature,
    candidatures.statut_candidature,
    candidatures.parcours_de_creation,
    candidatures.date_candidature_id,
    candidatures.date_traitement_id,
    candidatures.date_debut_contrat_id,
    candidatures.date_embauche_id,
    candidatures.candidature_archivee,
    candidatures.candidature_refusee_automatiquement,
    candidatures.injection_ai,
    candidatures.mode_attribution_pass_iae,
    candidatures.motif_refus,
    candidatures.nombre_fiches_de_poste,

    dest.type_etablissement      as destinataire_type_etablissement,
    dest.id_structure            as destinataire_id_structure,
    dest.nom_structure           as destinataire_nom_structure,
    dest.type_structure          as destinataire_type_structure,
    dest.siret                   as destinataire_siret,
    dest.code_naf                as destinataire_code_naf,
    dest.active                  as destinataire_active,
    dest.structure_conventionnee as destinataire_conventionnee,
    dest.code_commune            as destinataire_code_commune,
    dest.code_departement_insee  as destinataire_code_departement_insee,
    dest.nom_departement         as destinataire_nom_departement,
    dest.nom_departement_complet as destinataire_nom_departement_complet,
    dest.code_ft_clpe            as destinataire_code_clpe,
    dest.nom_clpe                as destinataire_nom_clpe,
    dest.code_region_insee       as destinataire_code_region_insee,
    dest.nom_region              as destinataire_nom_region,

    orig.type_etablissement      as origine_type_etablissement,
    orig.id_structure            as origine_id_structure,
    orig.id_organisation         as origine_id_organisation,
    orig.nom_structure           as origine_nom_structure,
    orig.nom_organisation        as origine_nom_organisation,
    orig.type_structure          as origine_type_structure,
    orig.type_organisation       as origine_type_organisation,
    orig.type_complet            as origine_type_complet,
    orig.habilitee               as origine_habilitee,
    orig.code_safir              as origine_code_safir,
    orig.siret                   as origine_siret,
    orig.active                  as origine_active,
    orig.structure_conventionnee as origine_conventionnee,
    orig.code_commune            as origine_code_commune,
    orig.code_departement_insee  as origine_code_departement_insee,
    orig.nom_departement         as origine_nom_departement,
    orig.nom_departement_complet as origine_nom_departement_complet,
    orig.code_ft_clpe            as origine_code_clpe,
    orig.nom_clpe                as origine_nom_clpe,
    orig.code_region_insee       as origine_code_region_insee,
    orig.nom_region              as origine_nom_region,

    candidatures.date_mise_a_jour_metabase,
    dest.is_siae                 as destinataire_is_siae

from candidatures
left join etablissements as dest
    on candidatures.id_structure_destinataire = dest.id
left join etablissements as orig
    on candidatures.id_etablissement_origine = orig.id
