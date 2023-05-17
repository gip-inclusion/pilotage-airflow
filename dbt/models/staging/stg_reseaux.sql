select
    {% if env_var('CI', '') %}
        siret,
    {% else %}
        {{ dbt_utils.star(source('oneshot', 'reseau_iae_adherents')) }},
    {% endif %}
    rid.id_institution
from {{ source('oneshot', 'reseau_iae_adherents') }} as ria
left join {{ source('oneshot', 'reseau_iae_ids') }} as rid
    on rid.nom = ria."réseau_iae"
