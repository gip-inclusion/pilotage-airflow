select
    {% if env_var('CI', ',') %}
        autopr_all.*,
    {% else %}
        {{ dbt_utils.star(ref('stg_autoprescription')) }},
    {% endif %}
    s.siret                 as siret,
    s.active                as active,
    s.ville                 as ville,
    s.nom_structure_complet as "nom_structure_complet"
from
    {{ ref('stg_autoprescription') }} as autopr_all
left join
    {{ ref('stg_structures') }} as s
    on autopr_all.id_structure = s.id
