select
    org.id                as id_org,
    org.siret             as siret_org_prescripteur,
    org."nom_département" as dept_org,  /*bien mettre nom département et pas département */
    org."région"          as "région_org",
    org."type"            as type_org_prescripteur
from
    {{ source('emplois', 'organisations') }} as org
