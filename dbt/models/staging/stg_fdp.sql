-- fiches de postes par structure
select
    fdpc.active,
    fdpc.id_fdp,
    fdpc.id_structure,
    fdpc.nom_rome_fdp,
    fdpc.siret_employeur,
    fdpc.type_structure,
    fdpc.categorie_structure,
    fdpc."nom_département_structure",
    fdpc."département_structure",
    fdpc."région_structure",
    app_geo.nom_epci                                        as epci_structure,
    app_geo.nom_zone_emploi                                 as bassin_emploi_structure,
    fdpc.nom_structure,
    fdpc."date_création_fdp",
    rome.domaine_professionnel,
    rome.grand_domaine,
    fdpc.nom_rome_fdp                                       as "métier",
    count(distinct fdpc.id_fdp)                             as nb_fdp_struct,
    min(fdpc.date_candidature)                              as date_1ere_candidature,
    max(fdpc.date_candidature)                              as date_derniere_candidature_recue,
    max(fdpc.date_embauche)                                 as date_derniere_embauche,
    (current_date - fdpc."date_création_fdp")               as delai_mise_en_ligne,
    /* une candidature a été recue dans les 30 derniers jours s'il
    existe une date candidature pas null et comprise entre aujd et le mois dernier */
    coalesce(
        max(fdpc.date_candidature) >= current_date - interval '30 days',
        not max(fdpc.date_candidature) is null,
        true
    )                                                       as candidature_30_derniers_jours,
    /* une embauche a été realisée dans les 30 derniers jours
    s'il existe une date embauche pas null et comprise entre aujd et le mois dernier */
    coalesce(
        max(fdpc.date_embauche) >= current_date - interval '30 days',
        not max(fdpc.date_embauche) is null,
        true
    )                                                       as embauche_30_derniers_jours,
    -- aucune candidature n'a été reçue
    coalesce(
        max(fdpc.date_candidature) is null,
        true
    )                                                       as aucune_candidatures_recues,
    -- une candidature a été refusée dans les 30 derniers jours pour motif pas de poste ouvert
    coalesce(
        true = any(array_agg(fdpc.refus_30_jours_pas_de_poste)),
        true
    )                                                       as refus_30_jours_pas_de_poste,
    (min(fdpc.date_candidature) - fdpc."date_création_fdp") as delai_crea_1ere_candidature,
    concat(fdpc.code_rome_fpd, '-', fdpc.nom_rome_fdp)      as rome
from
    {{ ref('stg_fdp_candidatures') }} as fdpc
left join {{ ref('code_rome_domaine_professionnel') }} as rome
    on fdpc.code_rome_fpd = rome.code_rome
left join {{ ref('structures') }} as s
    on fdpc.id_structure = s.id
left join {{ ref('stg_insee_appartenance_geo_communes') }} as app_geo
    on s.code_commune = app_geo.code_insee
group by
    fdpc.active,
    fdpc.id_fdp,
    fdpc.id_structure,
    fdpc.nom_rome_fdp,
    fdpc.siret_employeur,
    fdpc.type_structure,
    fdpc.categorie_structure,
    fdpc."nom_département_structure",
    fdpc."département_structure",
    fdpc."région_structure",
    app_geo.nom_epci,
    app_geo.nom_zone_emploi,
    fdpc.nom_structure,
    fdpc."date_création_fdp",
    rome.domaine_professionnel,
    rome.grand_domaine,
    concat(fdpc.code_rome_fpd, '-', fdpc.nom_rome_fdp),
    s.ville
