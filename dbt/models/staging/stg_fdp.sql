-- fiches de postes par structure
select
    fdpc.active,
    fdpc.id_fdp,
    fdpc.id_structure,
    fdpc.nom_rome_fdp,
    fdpc.siret_employeur,
    fdpc.type_structure,
    fdpc."nom_département_structure",
    fdpc."département_structure",
    fdpc."région_structure",
    bassin_emploi.nom_epci                                  as epci_structure,
    bassin_emploi.bassin_d_emploi                           as bassin_emploi_structure,
    fdpc.nom_structure,
    fdpc."date_création_fdp",
    rome.domaine_professionnel,
    rome.grand_domaine,
    count(distinct fdpc.id_fdp)                             as nb_fdp_struct,
    min(fdpc.date_candidature)                              as date_1ere_candidature,
    max(fdpc.date_candidature)                              as date_derniere_candidature_recue,
    max(fdpc.date_embauche)                                 as date_derniere_embauche,
    (current_date - fdpc."date_création_fdp")               as delai_mise_en_ligne,
    -- une candidature a été recue dans les 30 derniers jours s'il existe une date candidature pas null et comprise entre aujd et le mois dernier
    coalesce(
        max(fdpc.date_candidature) >= date_trunc('month', current_date) - interval '1 month',
        not max(fdpc.date_candidature) is null,
        true
    )                                                       as candidature_30_derniers_jours,
    -- une embauche a été realisée dans les 30 derniers jours s'il existe une date embauche pas null et comprise entre aujd et le mois dernier
    coalesce(
        max(fdpc.date_embauche) >= date_trunc('month', current_date) - interval '1 month',
        not max(fdpc.date_embauche) is null,
        true
    )                                                       as embauche_30_derniers_jours,
    -- aucune candidature n'a été reçue
    -- incorrect -> regarder si array agg date candidature ne contient que des null
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
left join {{ source('emplois', 'structures') }} as s
    on fdpc.id_structure = s.id
left join {{ ref('stg_bassin_emploi') }} as bassin_emploi
    on bassin_emploi.id_structure = s.id
group by
    fdpc.active,
    fdpc.id_fdp,
    fdpc.id_structure,
    fdpc.nom_rome_fdp,
    fdpc.siret_employeur,
    fdpc.type_structure,
    fdpc."nom_département_structure",
    fdpc."département_structure",
    fdpc."région_structure",
    bassin_emploi.nom_epci,
    bassin_emploi.bassin_d_emploi,
    fdpc.nom_structure,
    fdpc."date_création_fdp",
    rome.domaine_professionnel,
    rome.grand_domaine,
    concat(code_rome_fpd, '-', nom_rome_fdp),
    s.ville
