select
    id,
    department_code
from {{ source('raw_marche', 'perimeters_perimeter') }}
