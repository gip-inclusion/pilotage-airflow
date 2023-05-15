select distinct
    cd."état",
    c.age,
    c."total_critères_niveau_1",
    c."total_critères_niveau_2",
    c."critère_n1_bénéficiaire_du_rsa",
    c."critère_n1_allocataire_ass",
    c."critère_n1_allocataire_aah",
    c."critère_n1_detld_plus_de_24_mois",
    c."critère_n2_niveau_d_étude_3_cap_bep_ou_infra",
    c."critère_n2_senior_plus_de_50_ans",
    c."critère_n2_jeune_moins_de_26_ans",
    c."critère_n2_sortant_de_l_ase",
    c."critère_n2_deld_12_à_24_mois",
    c."critère_n2_travailleur_handicapé",
    c."critère_n2_parent_isolé",
    c."critère_n2_personne_sans_hébergement_ou_hébergée_ou_ayant_u",
    c."critère_n2_réfugié_statutaire_bénéficiaire_d_une_protectio",
    c."critère_n2_résident_zrr",
    c."critère_n2_résident_qpv",
    c."critère_n2_sortant_de_détention_ou_personne_placée_sous_main",
    c."critère_n2_maîtrise_de_la_langue_française",
    c."critère_n2_mobilité",
    c."département",
    c."nom_département",
    c."région",
    c.adresse_en_qpv,
    c.total_candidatures,
    c.total_embauches,
    c.date_diagnostic,
    c.id_auteur_diagnostic_employeur,
    c.type_auteur_diagnostic,
    c.sous_type_auteur_diagnostic,
    c.nom_auteur_diagnostic,
    cd.nom_structure,
    cd.type_structure,
    cd."département_structure",
    cd."nom_département_structure",
    cd."région_structure",
    cd.id_structure,
    (c.id),
    date_part('year', c.date_diagnostic) as "année_diagnostic",
    /* on considère que l'on a de l'auto prescription lorsque l'employeur est l'auteur du diagnostic et effectue l'embauche */
    /* En créant une colonne on peut comparer les candidatures classiques à l'auto prescription */
    case
        when
            c.type_auteur_diagnostic = 'Employeur'
            and cd.origine = 'Employeur'
            and c.id_auteur_diagnostic_employeur = cd.id_structure then 'Autoprescription'
        else 'parcours classique'
    end                                  as type_de_candidature,
    case
        when c.injection_ai = 0 then 'Non'
        else 'Oui'
    end                                  as reprise_de_stock_ai_candidats
from
    {{ source('emplois', 'candidatures') }} as cd
left join {{ source('emplois', 'candidats') }} as c
    on cd.id_candidat = c.id
