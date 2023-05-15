select
    {% if env_var('CI', '') %}
        autopr_c.*,
    {% else %}
        /* Here, the addition of except = id then autopr_c.id was necessary because otherwise id was considered as ambiguous*/
        {{ dbt_utils.star(ref('stg_candidats_autoprescription'), except=["id"]) }},
        autopr_c.id,
    {% endif %}
    ac.total_candidats,
    s.nom_structure_complet as "nom_structure_complet"
from
    {{ ref('stg_candidats_autoprescription') }} as autopr_c
left join
    {{ ref('stg_candidats_count') }} as ac
    on autopr_c.id = ac.id
left join
    {{ ref('stg_structures') }} as s
    on autopr_c.id_structure = s.id
