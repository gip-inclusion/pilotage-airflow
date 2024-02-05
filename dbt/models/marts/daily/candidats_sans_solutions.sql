select
    c.id,
    max(c.région)           as "région_candidat",
    max(c.département)      as "département_candidat",
    min(date_diagnostic)    as date_diagnostic,
    -- attention ici c la premiere des candidatures envoyées les 6 derniers mois
    min(c.date_candidature) as date_premiere_candidature,
    max(c.date_candidature) as date_derniere_candidature,
    --max(diagnostic_valide)  as diagnostic_valide,
    --current_date - interval '30 days' as dbg_30,
    -- candidat sans solution à 30 jours
    case
        when not exists (
            select 1
            from {{ ref('stg_candidatures_candidats_reels') }} as c2
            where
                c2.id = c.id
                -- a une candidature acceptée
                and c2."état" = 'Candidature acceptée'
                -- pendant les 30 jours après la réalisation du diag
                and c2.date_diagnostic + interval '30 days' > c2.date_candidature
        )
        -- et il y a déjà eu 30 jours écoulés depuis le diag
        and
        (current_date - interval '30 days'  > max(c.date_diagnostic)) then 1
        else 0
    end                     as sans_solution_30,
    -- candidat sans solution à 45 jours
    case
        when not exists (
            select 1
            from {{ ref('stg_candidatures_candidats_reels') }} as c2
            where
                c2.id = c.id
                and c2."état" = 'Candidature acceptée'
                and c2.date_diagnostic + interval '45 days' > c2.date_candidature
        )
        and
        (current_date - interval '45 days'  > max(c.date_diagnostic)) then 1
        else 0
    end                     as sans_solution_45,
    -- candidat sans solution à 60 jours
    case
        when not exists (
            select 1
            from {{ ref('stg_candidatures_candidats_reels') }} as c2
            where
                c2.id = c.id
                and c2."état" = 'Candidature acceptée'
                and c2.date_diagnostic + interval '60 days' > c2.date_candidature
        )
        and
        (current_date - interval '60 days'  > max(c.date_diagnostic)) then 1
        else 0
    end                     as sans_solution_60,
    -- candidat sans solution à 90 jours
    case
        when not exists (
            select 1
            from {{ ref('stg_candidatures_candidats_reels') }} as c2
            where
                c2.id = c.id
                and c2."état" = 'Candidature acceptée'
                and c2.date_diagnostic + interval '90 days' > c2.date_candidature
        )
        and
        (current_date - interval '90 days'  > max(c.date_diagnostic)) then 1
        else 0
    end                     as sans_solution_90
from {{ ref('stg_candidatures_candidats_reels') }} as c
where c.diagnostic_valide = 1
group by c.id
-- la candidature la plus récente a été émise il y a moins de 6 mois
-- ou aucune candidature n'a été émise depuis le diag
having max(c.date_candidature) >= current_date - interval '6 months' or max(c.date_candidature) is null
