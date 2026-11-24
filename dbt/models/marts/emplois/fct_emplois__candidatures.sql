with candidatures as (
    select *
    from {{ ref('stg_emplois__candidatures') }}
),

candidats as (
    select
        id,
        type_auteur_diagnostic,
        id_auteur_diagnostic_employeur
    from {{ ref('stg_emplois__candidats') }}
),

etablissements as (
    select *
    from {{ ref('dim_emplois__etablissements') }}
),

motifs_refus as (
    select *
    from {{ ref('stg_emplois__motifs_refus') }}
),

fiches_par_candidature as (
    select
        id_candidature,
        count(distinct id_fiche_de_poste) as nombre_fiches_de_poste
    from {{ ref('stg_emplois__fiche_de_poste_par_candidature') }}
    group by id_candidature
),

final as (
    select -- noqa: ST06
        candidature.id_candidature,
        candidature.id_candidat,
        structure_destinataire.id                                       as id_structure_destinataire,
        coalesce(organisation.id, structure_origine.id)                 as id_etablissement_origine,
        candidature.id_utilisateur_origine_candidature,
        candidature.origine_candidature,
        candidature.origine_detaillee,
        candidature.statut_candidature,
        candidature.parcours_de_creation,
        {{ date_id_from_date("candidature.date_candidature::date") }}   as date_candidature_id,
        {{ date_id_from_date("candidature.date_traitement::date") }}    as date_traitement_id,
        {{ date_id_from_date("candidature.date_debut_contrat::date") }} as date_debut_contrat_id,
        {{ date_id_from_date("candidature.date_embauche::date") }}      as date_embauche_id,
        candidature.candidature_archivee,
        candidature.candidature_refusee_automatiquement,
        candidature.injection_ai,
        candidature.mode_attribution_pass_iae,
        candidature.date_mise_a_jour_metabase,
        case
            when motif.label = 'Autre'
                then 'Autre (la SIAE a saisi le motif "autre")'
            else motif.label
        end                                                             as motif_refus,
        coalesce(fiches.nombre_fiches_de_poste, 0)                      as nombre_fiches_de_poste,
        case
            when
                candidat.type_auteur_diagnostic = 'Employeur'
                and candidat.id_auteur_diagnostic_employeur = candidature.id_structure_destinataire
                then 'Autoprescription'
            else 'Parcours classique'
        end                                                             as type_de_candidature
    from candidatures as candidature
    left join candidats as candidat
        on candidature.id_candidat = candidat.id
    left join etablissements as structure_destinataire
        on candidature.id_structure_destinataire = structure_destinataire.id_structure
    left join etablissements as organisation
        on candidature.id_organisation_prescriptrice = organisation.id_organisation
    left join etablissements as structure_origine
        on candidature.id_structure_origine = structure_origine.id_structure
    left join motifs_refus as motif
        on candidature.motif_refus = motif.code
    left join fiches_par_candidature as fiches
        on candidature.id_candidature = fiches.id_candidature
)

select *
from final
