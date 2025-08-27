select
    fdp."département_employeur"     as "département_structure",
    fdp."région_employeur"          as "région_structure",
    fdp.id_employeur                as id_structure,
    fdp."nom_département_employeur" as "nom_département_structure",
    s.nom                           as nom_structure,
    /* On récupère les infos via la table structure pour éviter
    des lignes vides lors de la recupération des infos candidatures */
    fdp.type_employeur              as type_structure,
    grp_strct.groupe                as categorie_structure,
    crdp.grand_domaine,
    crdp.domaine_professionnel,
    fdp.code_rome                   as code_rome_fpd,
    fdp."date_création"             as "date_création_fdp",
    fdp."date_mise_à_jour_metabase",
    cdd.motif_de_refus,
    cdd.date_candidature,
    cdd.date_embauche,
    fdp.id                          as id_fdp,
    fdp.nom_rome                    as nom_rome_fdp,
    fdpc.id_candidature,
    fdp.siret_employeur,
    -- la candidature a été refusée dans les 30 derniers jours pour motif pas de poste ouvert
    coalesce(
        cdd.date_candidature is not null
        and (
            cdd.date_candidature >= current_date - interval '30 days'
            and cdd.motif_de_refus = 'Pas de recrutement en cours'
        ),
        true
    )                               as refus_30_jours_pas_de_poste,
    -- fdp est active si le recrutement est actuellement ouvert
    coalesce(
        fdp.recrutement_ouvert = 1,
        true
    )                               as active
from {{ source('emplois', 'fiches_de_poste') }} as fdp
left join {{ source('emplois', 'fiches_de_poste_par_candidature') }} as fdpc
    on fdp.id = fdpc.id_fiche_de_poste
left join {{ source('emplois', 'candidatures') }} as cdd
    on fdpc.id_candidature = cdd.id
left join {{ ref('code_rome_domaine_professionnel') }} as crdp
    on fdp.code_rome = crdp.code_rome
left join {{ ref('structures') }} as s
    on fdp.id_employeur = s.id
left join {{ ref('groupes_structures') }} as grp_strct
    on fdp.type_employeur = grp_strct.structure
