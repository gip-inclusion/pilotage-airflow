select
    hash_nir,
    salarie_id
from {{ source("fluxIAE","fluxIAE_Salarie") }}
group by hash_nir, salarie_id
