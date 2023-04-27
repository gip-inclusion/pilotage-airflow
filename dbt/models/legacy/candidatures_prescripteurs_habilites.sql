with candidatures_ph as (
    select
        *,
        replace(cel.origine_détaillée, 'Prescripteur habilité ', '') as origine_simplifiee
    from {{ ref('candidatures_echelle_locale') }} as cel
    where starts_with(cel.origine_détaillée, 'Prescripteur habilité')
)
select
    candidatures_ph.*,
    org_libelles.libelle as libelle_complet
from candidatures_ph
left join {{ ref('organisation_libelles')}} as org_libelles
on org_libelles.type = candidatures_ph.origine_simplifiee