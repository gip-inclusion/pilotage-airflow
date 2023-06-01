with candidatures_ph as (
    select
        {{ pilo_star(ref('candidatures_echelle_locale')) }},
        replace("origine_détaillée", 'Prescripteur habilité ', '') as origine_simplifiee
    from {{ ref('candidatures_echelle_locale') }}
    where starts_with("origine_détaillée", 'Prescripteur habilité')
)
select
    candidatures_ph.*,
    org_libelles.libelle as libelle_complet
from candidatures_ph
left join {{ ref('organisations_libelles') }} as org_libelles
    on org_libelles.type = candidatures_ph.origine_simplifiee
