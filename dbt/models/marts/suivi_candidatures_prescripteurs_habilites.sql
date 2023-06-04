with candidatures_ph as (
    select
        {{ pilo_star(ref('candidatures_echelle_locale'),
                     except = ["nom_epci"]) }},
        replace("origine_détaillée", 'Prescripteur habilité ', '') as origine_simplifiee
    from {{ ref('candidatures_echelle_locale') }}
    where starts_with("origine_détaillée", 'Prescripteur habilité')
)
select
    candidatures_ph.*,
    organisations.epci   as nom_epci,
    org_libelles.libelle as libelle_complet
from candidatures_ph
left join {{ ref('organisations_libelles') }} as org_libelles
    on org_libelles.type = candidatures_ph.origine_simplifiee
left join {{ ref('stg_organisations') }} as organisations
    on organisations.id = candidatures_ph.id_org_prescripteur
