select
    id,
    type_inscription,
    diagnostic_valide,
    type_auteur_diagnostic,
    "région",
    "nom_département",
    coalesce(sum(case when "type_structure" in ('ACI', 'AI', 'EI', 'EITI', 'ETTI') then 1 else 0 end) > 0) as candidat_iae,
    max(date_candidature)                                                                                  as date_derniere_candidature,
    current_date - max(date_candidature)                                                                   as delai_derniere_candidature,
    current_date - max(case when "état" = 'Candidature acceptée' then date_candidature end)                as delai_derniere_candidature_acceptee,
    max(case when "état" = 'Candidature acceptée' then date_candidature end)                               as date_derniere_candidature_acceptee,
    sum(case when "état" = 'Candidature acceptée' then 1 else 0 end)                                       as nb_candidatures_acceptees,
    sum(case when "état" != 'Candidature acceptée' then 1 else 0 end)                                      as nb_candidatures_sans_accept,
    coalesce(sum(case when "état" = 'Candidature acceptée' then 1 else 0 end) > 0)                         as a_eu_acceptation
from {{ ref('stg_candidats_candidatures') }}
where date_candidature >= current_date - interval '6 months'
group by
    id,
    type_inscription,
    "région",
    "nom_département",
    diagnostic_valide,
    type_auteur_diagnostic