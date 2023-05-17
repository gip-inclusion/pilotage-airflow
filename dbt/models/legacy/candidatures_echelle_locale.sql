select
    {% if env_var('CI', '') %}
        id,
    {% else %}
        {{ dbt_utils.star(ref('stg_candidatures'), relation_alias='candidatures') }},
        {{ dbt_utils.star(ref('stg_org_prescripteur'), except=["id_org"], relation_alias='org_prescripteur') }},
        {{ dbt_utils.star(ref('stg_bassin_emploi'), except=["nom_departement","nom_region","type_epci","id_structure"] ,relation_alias='bassin_emploi') }},
        {{ dbt_utils.star(ref('stg_reseaux'), except=["SIRET","id_structure"] ,relation_alias='rsx') }},
    {% endif %}
    nom_org.type_auteur_diagnostic_detaille
from
    {{ ref('stg_candidatures') }} as candidatures
left join {{ ref('stg_bassin_emploi') }} as bassin_emploi
    on bassin_emploi.id_structure = candidatures.id_structure
left join {{ ref('stg_emmaus') }} as adherents_emmaus
    on adherents_emmaus.id_structure = candidatures.id_structure
left join {{ ref('stg_coorace') }} as adherents_coorace
    on adherents_coorace.id_structure = candidatures.id_structure
left join {{ ref('stg_fei') }} as adherents_fei
    on adherents_fei.id_structure = candidatures.id_structure
left join {{ ref('stg_unai') }} as adherents_unai
    on adherents_unai.id_structure = candidatures.id_structure
left join {{ ref('stg_cocagne') }} as adherents_cocagne
    on adherents_cocagne.id_structure = candidatures.id_structure
left join {{ ref('stg_org_prescripteur') }} as org_prescripteur
    on org_prescripteur.id_org = candidatures.id_org_prescripteur
left join {{ ref('stg_reseaux') }} as rsx
    on rsx.id_structure = candidatures.id_structure
left join {{ ref('nom_prescripteur') }} as nom_org
    on nom_org.origine_detaille = candidatures."origine_détaillée"
