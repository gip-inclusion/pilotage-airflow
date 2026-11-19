select
    code,
    label
from {{ source('raw_emplois', 'c1_ref_type_contrat') }}
