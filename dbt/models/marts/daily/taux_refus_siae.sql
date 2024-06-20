/* L'objectif est de suivre le taux de refus par type de structure */
with etp_conventionnes as (
    select
        type_siae,
        nom_departement_af,
        nom_region_af,
        sum("nombre_etp_conventionnés") as nombre_etp_conventionnes
    from {{ ref('nombre_etp_conventionnes') }}
    where annee_af = date_part('year', current_date)
    group by
        type_siae,
        nom_departement_af,
        nom_region_af
)

select
    /* Nombre de candidatures acceptées initiées par l'employeur de type SIAE */
    etp_conventionnes.nombre_etp_conventionnes,
    /* Nombre de candidatures initiées par l'employeur de type SIAE */
    cel.date_candidature,
    cel.type_structure,
    cel."nom_département_structure",
    cel."région_structure",
    cel.epci,
    cel.bassin_emploi_structure,
    cel.type_prescripteur,
    cel.origine,
    cel."origine_détaillée",
    cel.tranche_age,
    cel.genre_candidat,
    count(distinct cel.id)
    filter (
        where
        (cel.origine = 'Employeur')
        and (cel."état" = 'Candidature acceptée')
        and cel.type_structure in ('EI', 'ETTI', 'AI', 'ACI', 'EITI')
    )                                      as nombre_candidatures_acceptees_employeurs,
    count(distinct cel.id)
    filter (
        where
        (cel.origine = 'Employeur')
        and cel.type_structure in ('EI', 'ETTI', 'AI', 'ACI', 'EITI')
    )                                      as nombre_candidatures_employeurs,
    count(distinct cel.id)
    filter (
        where
        (cel."état" = 'Candidature acceptée')
        and cel.type_structure in ('EI', 'ETTI', 'AI', 'ACI', 'EITI')
    )                                      as nombre_candidatures_acceptees,
    count(distinct fdpc.id_fiche_de_poste) as nombre_fiches_poste_ouvertes,
    count(distinct cel.id)                 as nombre_candidatures,
    count(distinct cel.id)
    filter (
        where (cel."état" = 'Candidature refusée')
    )                                      as nombre_candidatures_refusees,
    count(distinct cel.id)
    filter (
        where (cel."état" = 'Candidature refusée') and cel.origine != 'Employeur'
    )                                      as nb_candidatures_refusees_non_emises_par_employeur_siae,
    count(distinct cel.id_structure)       as nombre_siae
from
    {{ ref('candidatures_echelle_locale') }} as cel
left join
    {{ source('emplois', 'fiches_de_poste_par_candidature') }} as fdpc
    on cel.id = fdpc.id_candidature
left join
    {{ source('emplois', 'fiches_de_poste') }} as fdp on fdpc.id_fiche_de_poste = fdp.id
left join
    {{ ref('structures') }} as structures on structures.id = cel.id_structure
left join
    etp_conventionnes
    on
        etp_conventionnes.type_siae = cel.type_structure
        and etp_conventionnes.nom_departement_af = cel."nom_département_structure"
        and etp_conventionnes.nom_region_af = cel."région_structure"
where
    cel.injection_ai = 0
    and fdp.recrutement_ouvert = 1
    /*se restreindre aux 12 derniers mois*/
    and cel.date_candidature >= date_trunc('month', cast((cast(now() as timestamp) + (interval '-12 month')) as timestamp))
    and cel.type_structure in ('EI', 'ETTI', 'AI', 'ACI', 'EITI')
group by
    etp_conventionnes.nombre_etp_conventionnes,
    cel.date_candidature,
    cel.type_structure,
    cel.categorie_structure,
    cel."nom_département_structure",
    cel."région_structure",
    cel.epci,
    cel.bassin_emploi_structure,
    cel.type_prescripteur,
    cel.origine,
    cel."origine_détaillée",
    cel.tranche_age,
    cel.genre_candidat
