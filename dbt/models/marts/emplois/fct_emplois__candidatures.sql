with candidatures as (
    select *
    from {{ ref('stg_emplois__candidatures') }}
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
    select
        candidature.id_candidature,
        candidature.id_candidat,
        destinataire.id                            as id_structure_destinataire,
        prescriptrice.id                           as id_organisation_prescriptrice,
        origine.id                                 as id_structure_origine,
        candidature.id_utilisateur_origine_candidature,
        candidature.origine_candidature,
        candidature.origine_detaillee,
        candidature.statut_candidature,
        candidature.parcours_de_creation,
        candidature.date_candidature,
        candidature.date_traitement,
        candidature.date_debut_contrat,
        candidature.date_embauche,
        candidature.candidature_archivee,
        candidature.candidature_refusee_automatiquement,
        candidature.injection_ai,
        candidature.mode_attribution_pass_iae,
        candidature.date_mise_a_jour_metabase,
        case
            when motif.label = 'Autre'
                then 'Autre (la SIAE a saisi le motif "autre")'
            else motif.label
        end                                        as motif_refus,
        coalesce(fiches.nombre_fiches_de_poste, 0) as nombre_fiches_de_poste
    from candidatures as candidature
    left join etablissements as destinataire
        on candidature.id_structure_destinataire = destinataire.id_structure
    left join etablissements as prescriptrice
        on candidature.id_organisation_prescriptrice = prescriptrice.id_organisation
    left join etablissements as origine
        on candidature.id_structure_origine = origine.id_structure
    left join motifs_refus as motif
        on candidature.motif_refus = motif.code
    left join fiches_par_candidature as fiches
        on candidature.id_candidature = fiches.id_candidature
)

select *
from final
