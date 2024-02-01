select
    c.id,
    min(date_diagnostic)    as date_diagnostic,
    min(c.date_candidature) as date_premiere_candidature,
    max(c.date_candidature) as date_derniere_candidature,
    case
        when not exists (
            select 1
            from {{ ref('stg_candidatures_candidats_entres') }} as c2
            where
                c2.id = c.id
                and c2."état" = 'Candidature acceptée'
                and c2.date_diagnostic + interval '30 days' > c2.date_candidature
        ) then 1
        else 0
    end                     as sans_solution_30,
    case
        when not exists (
            select 1
            from {{ ref('stg_candidatures_candidats_entres') }} as c2
            where
                c2.id = c.id
                and c2."état" = 'Candidature acceptée'
                and c2.date_diagnostic + interval '45 days' > c2.date_candidature
        ) then 1
        else 0
    end                     as sans_solution_45,
    case
        when not exists (
            select 1
            from {{ ref('stg_candidatures_candidats_entres') }} as c2
            where
                c2.id = c.id
                and c2."état" = 'Candidature acceptée'
                and c2.date_diagnostic + interval '60 days' > c2.date_candidature
        ) then 1
        else 0
    end                     as sans_solution_60,
    case
        when not exists (
            select 1
            from {{ ref('stg_candidatures_candidats_entres') }} as c2
            where
                c2.id = c.id
                and c2."état" = 'Candidature acceptée'
                and c2.date_diagnostic + interval '90 days' > c2.date_candidature
        ) then 1
        else 0
    end                     as sans_solution_90
from {{ ref('stg_candidatures_candidats_entres') }} as c
group by c.id
