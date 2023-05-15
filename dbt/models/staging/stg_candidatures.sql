select
    {% if env_var('CI', '') %}
        *
    {% else %}
        {{ dbt_utils.star(source('emplois', 'candidatures'), except=["état", "origine", "délai_de_réponse", "délai_prise_en_compte", "cdd_id_org_prescripteur", "cdd_id_structure"]) }},
        -- laurine: je mets tout ici car sinon dbt râle car je lui demande de créer des colonnes qui existent déjà
        id_structure                              as cdd_id_structure,
        id_org_prescripteur                       as cdd_id_org_prescripteur,
        case
            when "état" = 'Candidature déclinée' then 'Candidature refusée'
            else "état"
        end                                       as "état",
        case
            when origine = 'Candidat' then 'Candidature en ligne'
            else origine
        end                                       as origine,
        extract(day from "délai_de_réponse")      as temps_de_reponse,
        extract(day from "délai_prise_en_compte") as temps_de_prise_en_compte
    {% endif %}
from
    {{ source('emplois', 'candidatures') }}
