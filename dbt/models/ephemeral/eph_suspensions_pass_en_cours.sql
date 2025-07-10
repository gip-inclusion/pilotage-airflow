select *
from {{ ref('suspensions_pass') }}
where suspension_en_cours = 'Oui'
