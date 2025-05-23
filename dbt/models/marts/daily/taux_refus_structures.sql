select
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
    cel.genre_candidat,
    count(distinct cel.id) filter (where cel.origine = 'Employeur' and cel."état" = 'Candidature acceptée') as nombre_candidatures_acceptees_employeurs,
    count(distinct cel.id) filter (where cel.origine = 'Employeur')                                         as nombre_candidatures_employeurs,
    count(distinct cel.id) filter (where cel."état" = 'Candidature acceptée')                               as nombre_candidatures_acceptees,
    count(distinct fdpc.id_fiche_de_poste)                                                                  as nombre_fiches_poste_ouvertes,
    count(distinct cel.id)                                                                                  as nombre_candidatures,
    count(distinct cel.id) filter (where cel."état" = 'Candidature refusée')                                as nombre_candidatures_refusees,
    count(distinct cel.id) filter (where cel."état" = 'Candidature refusée' and cel.origine != 'Employeur') as nb_candidatures_refusees_non_emises_par_employeur_siae,
    count(distinct cel.id_structure)                                                                        as nombre_siae
from
    {{ ref('candidatures_echelle_locale') }} as cel
left join
    {{ source('emplois', 'fiches_de_poste_par_candidature') }} as fdpc
    on cel.id = fdpc.id_candidature
left join
    {{ source('emplois', 'fiches_de_poste') }} as fdp on fdpc.id_fiche_de_poste = fdp.id
left join
    {{ ref('structures') }} as structures on cel.id_structure = structures.id
where
    cel.injection_ai = 0
    and fdp.recrutement_ouvert = 1
    and cel.date_candidature >= date_trunc('month', now() - interval '12 month')
group by
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
