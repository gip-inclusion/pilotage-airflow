select
    cel.type_structure                                                                                               as "Type_structure",
    cel.nom_structure                                                                                                as "Nom structure",
    cel.nom_org_prescripteur                                                                                         as "Prescripteur",
    cel.motif_de_refus                                                                                               as "Motif de refus",
    cel.tranche_age                                                                                                  as "Tranche d'âge",
    cel.genre_candidat                                                                                               as "Genre candidat",
    case when cel."état" = 'Candidature acceptée' then 'oui' else 'non' end                                          as "Candidature acceptée",
    case when cel."état" = 'Candidature refusée' then 'oui' else 'non' end                                           as "Candidature refusée",
    case when cel."état" != 'Candidature refusée' and cel."état" != 'Candidature acceptée' then 'oui' else 'non' end as "Candidature en cours ou embauché ailleurs",
    case when cc."critère_n1_bénéficiaire_du_rsa" = 1 then 'oui' else 'non' end                                      as "Bénéficiaire du RSA",
    case when cc."critère_n2_travailleur_handicapé" = 1 then 'oui' else 'non' end                                    as "Travailleur handicapé",
    date_part('month', cel.date_candidature)                                                                         as "Mois de l'année 2024",
    count(cel.id)                                                                                                    as "Nombre de candidatures"

from {{ ref('candidatures_echelle_locale') }} as cel left join {{ ref('stg_candidats_candidatures') }} as cc on cel.id = cc.id_candidature
where
    date_part('year', cel.date_candidature) = 2024
    and date_part('month', cel.date_candidature) <= 6
    and cel.dept_org = '50 - Manche'
group by
    date_part('month', cel.date_candidature),
    cel.type_structure,
    cel.nom_structure,
    cel.nom_org_prescripteur,
    cel."état",
    cel.motif_de_refus,
    cel.tranche_age,
    cel.genre_candidat,
    cc."critère_n1_bénéficiaire_du_rsa",
    cc."critère_n2_travailleur_handicapé"
