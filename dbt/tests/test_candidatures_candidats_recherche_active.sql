with nb_candidatures as (
    select
        (
            select count(*)
            from candidatures_echelle_locale
            where date_candidature >= current_date - interval '6 months'
        ) as candidatures_cel,
        (
            select count(*)
            from candidatures_candidats_recherche_active
        ) as candidatures_cra
)

select *
from nb_candidatures
where candidatures_cel != candidatures_cra
