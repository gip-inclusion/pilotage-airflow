select
    c.id                                   as id_candidat,
    cd.id                                  as id_candidature,
    c.hash_nir                             as hash_nir,
    c.age                                  as age,
    c."département"                        as "département",
    c."nom_département"                    as "nom_département",
    c."région"                             as "région",
    c.adresse_en_qpv                       as adresse_en_qpv,
    c.total_candidatures                   as total_candidatures,
    c.total_embauches                      as total_embauches,
    c.date_diagnostic                      as date_diagnostic,
    cd.date_candidature                    as date_candidature,
    c.id_auteur_diagnostic_employeur       as id_auteur_diagnostic_employeur,
    c.type_auteur_diagnostic               as type_auteur_diagnostic,
    c.sous_type_auteur_diagnostic          as sous_type_auteur_diagnostic,
    c.nom_auteur_diagnostic                as nom_auteur_diagnostic,
    cd."état"                              as "état",
    cd.id_structure                        as id_structure,
    cd.origine                             as origine,
    cd."origine_détaillée"                 as "origine_détaillée",
    cd.type_structure                      as type_structure,
    cd.nom_structure                       as nom_structure,
    cd."département_structure"             as "département_structure",
    cd."nom_département_structure"         as "nom_département_structure",
    cd."région_structure"                  as "région_structure",
    date_part('year', c.date_diagnostic)   as "année_diagnostic",
    date_part('year', cd.date_candidature) as "année_candidature",
    /* on considère que l'on a de l'auto prescription lorsque
    l'employeur est l'auteur du diagnostic et effectue l'embauche */
    /* En créant une colonne on peut comparer les candidatures classiques à l'auto prescription */
    case
        when
            c.type_auteur_diagnostic = 'Employeur'
            and c.id_auteur_diagnostic_employeur = cd.id_structure then 'Autoprescription'
        else 'parcours classique'
    end                                    as type_de_candidature,
    case
        when c.injection_ai = 0 then 'Non'
        else 'Oui'
    end                                    as reprise_de_stock_ai_candidats,
    case
        when cd.injection_ai = 0 then 'Non'
        else 'Oui'
    end                                    as reprise_de_stock_ai_candidatures
from
    {{ source('emplois', 'candidatures') }} as cd
left join
    {{ source('emplois', 'candidats') }} as c
    on cd.id_candidat = c.id
