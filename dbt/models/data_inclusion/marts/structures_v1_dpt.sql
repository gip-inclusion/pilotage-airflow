select
    {{ pilo_star(source('data_inclusion','structures_v1'), relation_alias = 'structures') }},
    geo.nom_departement,
    case
        when geo.code_dept like '%97%' then lpad(geo.code_dept, 3, '0')
        else lpad(geo.code_dept, 2, '0')
    end as code_dept
from {{ source('data_inclusion', 'structures_v1') }} as structures
left join {{ ref('stg_insee_appartenance_geo_communes') }} as geo
    on ltrim(structures.code_insee, '0') = geo.code_insee
