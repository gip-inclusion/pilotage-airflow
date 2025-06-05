select
    asp_benef.numero_annexe_financiere                                        as af_numero_annexe_financiere,
    asp_benef.annee_asp                                                       as emi_sme_annee,
    asp_benef."Mesure"                                                        as type_structure_emplois,
    asp_benef.denomination_asp                                                as structure_denomination,
    asp_benef.siret_asp                                                       as siret_structure,
    nom_departement_af,
    nom_region_af,
    asp_benef."2 - Nombre total des salariés en insertion ayant travaillé da" as nombre_de_beneficiaires,
    asp_benef."2 - Dont hommes "                                              as dont_hommes,
    asp_benef."2 - Dont femmes "                                              as dont_femmes,
    asp_benef."2 - Dont bénéficiaires du RSA "                                as dont_beneficiaires_du_rsa,
    asp_benef."2 - Dont bénéficiaires de l'ASS "                              as dont_beneficiaires_de_ass,
    asp_benef."2 - Dont bénéficiaires de l'AAH "                              as dont_beneficiaires_de_aah,
    asp_benef."2 - Dont nombre de personnes reconnues RQTH :"                 as dont_beneficiaires_de_rqth,
    asp_benef."2 - Dont résidants en ZRR "                                    as dont_beneficiaires_en_zrr,
    asp_benef."2 - Dont résidants en QPV "                                    as dont_beneficiaires_en_qpv,
    asp_benef."2 - Dont jeunes de moins de 26 ans :"                          as dont_beneficiaires_jeunes,
    asp_benef."2 - Dont personnes sans emploi de 50 ans et plus :"            as dont_beneficiaires_seniors,
    asp_benef."2 - Dont inscrits à Pôle emploi depuis 12 à 23 mois :"         as dont_beneficiaires_deld,
    asp_benef."2 - Dont inscrits à Pôle emploi depuis 24 mois et plus:"       as dont_beneficiaires_detld,
    asp_benef."2 - Dont personnes avec un niveau CAP – BEP"                   as dont_beneficiaires_formation_cap_bep,
    asp_benef."2 - Dont personnes avec qualifications non certifiantes"       as dont_beneficiaires_formation_non_certifiante,
    asp_benef."2 - Dont personnes avec un niveau inférieur au CAP"            as dont_beneficiaires_formation_inf_cap,
    asp_benef."4 - Total des sorties examinées AI ETTI EITI"                  as total_sorties,
    asp_benef."5 - Total des sorties dans l'emploi durable"                   as total_sorties_emploi_durable,
    asp_benef."6 - Total des sorties dans l'emploi de transition"             as total_sorties_emploi_transition,
    asp_benef."7 - Total des sorties positives"                               as total_sorties_emploi_positives,
    'extraction ponctuelle ASP'                                               as source_donnees
from
    {{ ref('fluxIAE_BenefSorties_ASP_v2') }} as asp_benef
union all
select
    {{ pilo_star(ref('int_beneficiaires_sorties'),  relation_alias="benef_current_year") }},
    null                                                  as total_sorties,
    null                                                  as total_sorties_emploi_durable,
    null                                                  as total_sorties_emploi_transition,
    null                                                  as total_sorties_emploi_positives,
    'extraction fluxIAE toutes structures année en cours' as source_donnees
from
    {{ ref('int_beneficiaires_sorties') }} as benef_current_year
where benef_current_year.emi_sme_annee = date_part('year', current_date)::INTEGER
union all
select
    {{ pilo_star(ref('int_beneficiaires_sorties'),  relation_alias="benef_eiti") }},
    null                                           as total_sorties,
    null                                           as total_sorties_emploi_durable,
    null                                           as total_sorties_emploi_transition,
    null                                           as total_sorties_emploi_positives,
    'extraction fluxIAE EITI avant année en cours' as source_donnees
from
    {{ ref('int_beneficiaires_sorties') }} as benef_eiti
where
    benef_eiti.emi_sme_annee != date_part('year', current_date)::INTEGER
    and benef_eiti.type_structure_emplois = 'EITI'
