select distinct on (acc_afi_id)
    {{ pilo_star(source("fluxIAE","fluxIAE_Accompagnement")) }},
    extract(year from to_date(acc_date_creation, 'dd/mm/yyyy')) as annee_creation_accompagnement
from {{ source("fluxIAE","fluxIAE_Accompagnement") }}
order by acc_afi_id asc, acc_date_modification desc
