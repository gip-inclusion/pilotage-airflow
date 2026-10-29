select
    code,
    label
from {{ source('raw_emplois', 'c1_ref_motif_de_refus') }}
