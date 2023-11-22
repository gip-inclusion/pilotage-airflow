select
    {{ pilo_star(ref('candidatures_echelle_locale'), relation_alias='candidatures') }},
    organisations."département"     as "département_prescripteur",
    organisations."nom_département" as "nom_département_prescripteur",
    organisations."région"          as "région_prescripteur",
    organisations.epci              as epci_prescripteur,
    organisations.zone_emploi       as zone_emploi_prescripteur,
    organisations.type_complet      as libelle_complet
from
    {{ ref('candidatures_echelle_locale') }} as candidatures
left join {{ ref('organisations') }} as organisations
    on organisations.id = candidatures.id_org_prescripteur
