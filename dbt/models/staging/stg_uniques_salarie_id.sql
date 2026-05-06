select
    hash_nir,
    salarie_id
from {{ ref("fluxIAE_Salarie_v2") }}
group by hash_nir, salarie_id
