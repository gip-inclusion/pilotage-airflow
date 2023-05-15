select
    {% if env_var('CI', '') %}
        id,
    {% else %}
        {{ dbt_utils.star(source('emplois', 'candidatures'), except=["état", "origine", "délai_de_réponse", "délai_prise_en_compte"]) }},
    {% endif %}
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
from
    {{ source('emplois', 'candidatures') }}
