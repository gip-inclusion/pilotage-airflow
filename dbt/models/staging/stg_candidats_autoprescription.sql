select distinct
    {% if env_var('CI', '') %}
        id,
    {% else %}
        {{ dbt_utils.star(source('emplois', 'candidats'), relation_alias="c") }},
    {% endif %}
    cd."état",
    cd.nom_structure,
    cd.type_structure,
    cd."département_structure",
    cd."nom_département_structure",
    cd."région_structure",
    cd.id_structure,
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
