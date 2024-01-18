/*
L'objectif est de calculer le délai entre la 1ère candidature et l'embauche des candidats orientés par PE:
- le nombre de candidats orientés par des prescripteurs habilités
    qui ont trouvé un emploi en IAE moins d 1 mois après leur première candidature
- le nombre de candidats orientés par des prescripteurs habilités
    qui ont trouvé un emploi en IAE entre 1 mois et 2 mois après leur première candidature
- le nombre de candidats orientés par des prescripteurs habilités
    qui ont trouvé un emploi en IAE entre 2 mois et 3 mois après leur première candidature
- le nombre de candidats orientés par des prescripteurs habilités
    qui ont trouvé un emploi en IAE entre 3 mois et 4 mois après leur première candidature
- le nombre de candidats orientés par des prescripteurs habilités
    qui ont trouvé un emploi en IAE entre 4 mois et 5 mois après leur première candidature
- le nombre de candidats orientés par des prescripteurs habilités
    qui ont trouvé un emploi en IAE entre 5 mois et 6 mois après leur première candidature
*/

/* Trouver la date de la première candidature + date de la première embauche */

with date_1ere_candidature as (
    select
        c.id_candidat,
        candidats."nom_département"                               as "nom_département_candidat",
        c.date_candidature,
        c.date_embauche,
        c.origine,
        candidats."type_structure_dernière_embauche",
        c.id_org_prescripteur,
        c.nom_org_prescripteur,
        c.id_structure,
        c.type_structure,
        c.nom_structure,
        c.nom_complet_structure,
        c."région_structure",
        c."nom_département_structure",
        coalesce(c.type_avec_habilitation, c."origine_détaillée") as "origine_détaillée",
        min(c.date_candidature)                                   as date_1ere_candidature,
        min(
            coalesce(c.date_embauche, '2099-01-01')
        )                                                         as date_1ere_embauche
    from
        {{ ref('candidatures_echelle_locale') }} as c
    inner join {{ ref('candidats') }} as candidats on c.id_candidat = candidats.id
    group by
        c.id_candidat,
        candidats."nom_département",
        c.date_candidature,
        c.date_embauche,
        c.origine,
        c.type_avec_habilitation,
        c."origine_détaillée",
        c.nom_org_prescripteur,
        c.id_structure,
        c.type_structure,
        c.nom_structure,
        c.nom_complet_structure,
        c."région_structure",
        c."nom_département_structure",
        candidats."type_structure_dernière_embauche",
        c.id_org_prescripteur
),

prescripteurs as (
    select
        id,
        /* Ajout du département du prescripteur pour les TBs privés */
        "nom_département" as "nom_département_prescripteur"
    from {{ ref('stg_organisations') }}
)

select
    date_cddr.id_candidat,
    date_cddr."nom_département_candidat",
    date_cddr.date_candidature,
    date_cddr.date_embauche,
    date_cddr.origine,
    date_cddr."origine_détaillée",
    date_cddr.nom_org_prescripteur,
    date_cddr.id_structure,
    date_cddr.type_structure,
    date_cddr.nom_structure,
    date_cddr.nom_complet_structure,
    date_cddr."région_structure",
    date_cddr."nom_département_structure",
    date_cddr.id_org_prescripteur,
    p."nom_département_prescripteur",
    date_cddr."type_structure_dernière_embauche",
    case
        /* Division /30 pour passer du nombre de jour au mois */
        when (
            (date_cddr.date_1ere_embauche - date_cddr.date_1ere_candidature)
            / 30
        ) < 1 then 'a- Moins d un mois'
        when (
            (date_cddr.date_1ere_embauche - date_cddr.date_1ere_candidature)
            / 30
        ) >= 1
        and ((date_cddr.date_1ere_embauche - date_cddr.date_1ere_candidature) / 30) < 2 then 'b- Entre 1 et 2 mois'
        when (
            (date_cddr.date_1ere_embauche - date_cddr.date_1ere_candidature) / 30
        ) >= 2
        and ((date_cddr.date_1ere_embauche - date_cddr.date_1ere_candidature) / 30) < 3 then 'c- Entre 2 et 3 mois'
        when (
            (date_cddr.date_1ere_embauche - date_cddr.date_1ere_candidature) / 30
        ) >= 3
        and ((date_cddr.date_1ere_embauche - date_cddr.date_1ere_candidature) / 30) < 4 then 'd- Entre 3 et 4 mois'
        when (
            (date_cddr.date_1ere_embauche - date_cddr.date_1ere_candidature) / 30
        ) >= 4
        and ((date_cddr.date_1ere_embauche - date_cddr.date_1ere_candidature) / 30) < 5 then 'e- Entre 4 et 5 mois'
        when (
            (date_cddr.date_1ere_embauche - date_cddr.date_1ere_candidature) / 30
        ) >= 5
        and ((date_cddr.date_1ere_embauche - date_cddr.date_1ere_candidature) / 30) < 6 then 'f- Entre 5 et 6 mois'
        when (
            (date_cddr.date_1ere_embauche - date_cddr.date_1ere_candidature) / 30
        ) >= 6 then 'g- 6 mois et plus'
    end as "délai_embauche"
from
    date_1ere_candidature as date_cddr
left join prescripteurs as p
    on p.id = date_cddr.id_org_prescripteur
/* Ecarter les candidats qui ne sont pas recrutés à aujourd'hui */
where date_cddr.date_1ere_embauche != '2099-01-01'
