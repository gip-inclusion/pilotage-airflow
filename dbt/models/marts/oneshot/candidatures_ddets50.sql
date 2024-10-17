select
    cel.date_candidature                                                          as "Date candidature",
    cel.type_structure                                                            as "Type_structure",
    cel.nom_structure                                                             as "Nom structure",
    cel.nom_epci_structure                                                        as "EPCI de la structure",
    cel.nom_org_prescripteur                                                      as "Prescripteur",
    cel.tranche_age                                                               as "Tranche d'âge",
    cel.genre_candidat                                                            as "Genre candidat",
    cel."état"                                                                    as "Etat de la candidature",
    cel.motif_de_refus                                                            as "Motif de refus",
    case when cc."critère_n1_bénéficiaire_du_rsa" = 1 then 'oui' else 'non' end   as "Bénéficiaire du RSA",
    case when cc."critère_n2_travailleur_handicapé" = 1 then 'oui' else 'non' end as "Travailleur handicapé"
from {{ ref('candidatures_echelle_locale') }} as cel
left join {{ ref('stg_candidats_candidatures') }} as cc on cel.id = cc.id_candidature
where
    date_part('year', cel.date_candidature) = 2024
    and date_part('month', cel.date_candidature) <= 6
    and cel.dept_org = '50 - Manche'
