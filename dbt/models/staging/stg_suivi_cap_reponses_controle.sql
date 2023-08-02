-- todo (laurine) simplifier la requête imbriquée redondante (dbt template ?)
select
    {{ pilo_star(source('emplois', 'cap_structures')) }},
    'NON' as "réponse_au_contrôle"
from cap_structures
left join (
    select distinct cap_candidatures.id_cap_structure
    from "cap_critères_iae"
    inner join cap_candidatures
        on "cap_critères_iae".id_cap_candidature = cap_candidatures.id
) as subquery
    on cap_structures.id = subquery.id_cap_structure
where subquery.id_cap_structure is null

union

select
    {{ pilo_star(source('emplois', 'cap_structures')) }},
    'OUI' as "réponse_au_contrôle"
from cap_structures
left join (
    select distinct cap_candidatures.id_cap_structure
    from "cap_critères_iae"
    inner join cap_candidatures
        on "cap_critères_iae".id_cap_candidature = cap_candidatures.id
) as subquery
    on cap_structures.id = subquery.id_cap_structure
where subquery.id_cap_structure is not null
