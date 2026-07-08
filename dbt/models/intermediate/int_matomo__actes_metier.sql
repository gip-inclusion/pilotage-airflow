with monthly_visits as (
    select *
    from {{ ref('stg_matomo__monthly_visits') }}
),

department_visits as (
    select
        mois,
        id_site,
        segment,
        departement,
        sum(nb_visits) as department_nb_visits
    from {{ ref('stg_matomo__monthly_department_visits') }}
    group by
        mois,
        id_site,
        segment,
        departement
),

employer_totals as (
    select
        mois,
        id_site,
        segment,
        nb_visits as total_nb_visits
    from monthly_visits
    where segment = 'pageUrl=@/search/employers/results'
),

employer_department_weights as (
    select
        employer_totals.mois,
        employer_totals.id_site,
        employer_totals.segment,
        employer_totals.total_nb_visits,
        department_visits.departement,
        department_visits.department_nb_visits,
        sum(department_visits.department_nb_visits) over (
            partition by employer_totals.mois, employer_totals.id_site, employer_totals.segment
        ) as total_department_nb_visits
    from employer_totals
    inner join department_visits
        on
            employer_totals.mois = department_visits.mois
            and employer_totals.id_site = department_visits.id_site
            and employer_totals.segment = department_visits.segment
),

employer_department_allocations as (
    select
        mois,
        id_site,
        segment,
        departement,
        floor(total_nb_visits * department_nb_visits::numeric / nullif(total_department_nb_visits, 0))::integer
            as nombre_actes
    from employer_department_weights
    where total_department_nb_visits > 0
),

employer_residual as (
    select
        employer_totals.mois,
        employer_totals.id_site,
        employer_totals.segment,
        'Inconnu'                                                                                        as departement,
        employer_totals.total_nb_visits - coalesce(sum(employer_department_allocations.nombre_actes), 0)
            as nombre_actes
    from employer_totals
    left join employer_department_allocations
        on
            employer_totals.mois = employer_department_allocations.mois
            and employer_totals.id_site = employer_department_allocations.id_site
            and employer_totals.segment = employer_department_allocations.segment
    group by
        employer_totals.mois,
        employer_totals.id_site,
        employer_totals.segment,
        employer_totals.total_nb_visits
),

employer_actes as (
    select
        mois,
        'emplois'                    as source,
        'Recherche d’offre d’emploi' as type_acte,
        'Support'                    as categorie_acte,
        false                        as north_star,
        false                        as north_star_70,
        false                        as traite,
        'Inconnu'                    as type_structure,
        departement,
        nombre_actes
    from employer_department_allocations

    union all

    select
        mois,
        'emplois'                    as source,
        'Recherche d’offre d’emploi' as type_acte,
        'Support'                    as categorie_acte,
        false                        as north_star,
        false                        as north_star_70,
        false                        as traite,
        'Inconnu'                    as type_structure,
        departement,
        nombre_actes
    from employer_residual
),

service_actes as (
    select
        mois,
        'emplois'                                             as source,
        'Recherche d’offre de service, hors emploi solidaire' as type_acte,
        'Support'                                             as categorie_acte,
        false                                                 as north_star,
        false                                                 as north_star_70,
        false                                                 as traite,
        'Inconnu'                                             as type_structure,
        'Inconnu'                                             as departement,
        nb_visits                                             as nombre_actes
    from monthly_visits
    where segment = 'pageUrl=@/search/services/results'
)

select *
from employer_actes
where nombre_actes > 0

union all

select *
from service_actes
where nombre_actes > 0
