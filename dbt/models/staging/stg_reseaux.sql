select
    {% if env_var('CI', '') %}
        siret,
    {% else %}
        {{ dbt_utils.star(source('oneshot', 'reseau_iae_adherents')) }},
    {% endif %}
    s.id as id_structure,
    rid.id_institution
from {{ source('oneshot', 'reseau_iae_adherents') }} as ria
left join {{ ref('reseau_iae_ids') }} as rid
    on rid.nom = ria."Réseau IAE"
inner join {{ source('emplois', 'structures') }} as s
    on s.siret = ria."SIRET"
