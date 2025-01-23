-- une fiche de poste est active si le recrutement est actuellement ouvert
select
    {{ pilo_star(ref('stg_fdp')) }}
from {{ ref('stg_fdp') }}
where active
