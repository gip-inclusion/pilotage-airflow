select *
from {{ ref('int_fagerh__prestations_publics') }}
where
    public_dimension = 'type_handicap'
    and public_subdimension is null
