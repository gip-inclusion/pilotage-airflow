select
    {{ pilo_star(source('emplois', 'organisations')) }},
    case
        when "habilitée" = 1 then concat('Prescripteur habilité ', "type")
        when "habilitée" = 0 then concat('Orienteur ', "type")
    end as type_complet_avec_habilitation
from {{ source('emplois', 'organisations') }}
