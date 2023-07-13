select {{ pilo_star(source('emplois', 'organisations')) }}
from {{ source('emplois', 'organisations') }}
where brsa = 1 or type = 'ODC' or type = 'DEPT'
