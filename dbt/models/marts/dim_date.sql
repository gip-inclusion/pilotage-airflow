with jours as (
    select date '{{ var("dim_date_start_date") }}' + decalage as date_jour
    from generate_series(
        0, date '{{ var("dim_date_end_date") }}' - date '{{ var("dim_date_start_date") }}'
    ) as serie (decalage)
),

calendrier as (
    select
        date_jour,
        extract(day from date_jour)::integer              as jour_du_mois,
        extract(doy from date_jour)::integer              as jour_de_annee,
        extract(isodow from date_jour)::integer           as jour_semaine_iso,
        extract(week from date_jour)::integer             as semaine_iso,
        extract(isoyear from date_jour)::integer          as annee_iso,
        extract(month from date_jour)::integer            as mois,
        extract(quarter from date_jour)::integer          as trimestre,
        extract(year from date_jour)::integer             as annee,
        date_trunc('month', date_jour::timestamp)::date   as date_debut_mois,
        date_trunc('quarter', date_jour::timestamp)::date as date_debut_trimestre,
        date_trunc('year', date_jour::timestamp)::date    as date_debut_annee
    from jours
),

libelles as (
    select
        *,
        (array['lundi', 'mardi', 'mercredi', 'jeudi', 'vendredi', 'samedi', 'dimanche'])[jour_semaine_iso] as nom_jour,
        (array[
            'janvier', 'février', 'mars', 'avril', 'mai', 'juin',
            'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre'
        ])[mois]                                                                                           as nom_mois
    from calendrier
)

select
    annee * 10000 + mois * 100 + jour_du_mois                                   as date_id,
    date_jour,
    jour_du_mois || ' ' || nom_mois || ' ' || annee                             as libelle_date,
    false                                                                       as est_date_inconnue,
    jour_du_mois,
    jour_de_annee,
    jour_semaine_iso,
    nom_jour,
    semaine_iso,
    annee_iso,
    annee_iso || '-W' || lpad(semaine_iso::text, 2, '0')                        as annee_semaine_iso,
    date_jour - (jour_semaine_iso - 1)                                          as date_debut_semaine,
    date_jour + (7 - jour_semaine_iso)                                          as date_fin_semaine,
    mois,
    nom_mois,
    annee || '-' || lpad(mois::text, 2, '0')                                    as annee_mois,
    date_debut_mois,
    (date_debut_mois + interval '1 month' - interval '1 day')::date             as date_fin_mois,
    trimestre,
    annee || '-T' || trimestre                                                  as annee_trimestre,
    annee,
    date_debut_trimestre,
    (date_debut_trimestre + interval '3 months' - interval '1 day')::date       as date_fin_trimestre,
    date_debut_annee,
    (date_debut_annee + interval '1 year' - interval '1 day')::date             as date_fin_annee,
    jour_semaine_iso in (6, 7)                                                  as est_weekend,
    date_jour = (date_debut_mois + interval '1 month' - interval '1 day')::date as est_fin_de_mois,
    annee % 4 = 0 and (annee % 100 != 0 or annee % 400 = 0)                     as est_annee_bissextile
from libelles

union all

select
    -1::integer           as date_id,
    null::date            as date_jour,
    'Date inconnue'::text as libelle_date,
    true                  as est_date_inconnue,
    null::integer         as jour_du_mois,
    null::integer         as jour_de_annee,
    null::integer         as jour_semaine_iso,
    null::text            as nom_jour,
    null::integer         as semaine_iso,
    null::integer         as annee_iso,
    null::text            as annee_semaine_iso,
    null::date            as date_debut_semaine,
    null::date            as date_fin_semaine,
    null::integer         as mois,
    null::text            as nom_mois,
    null::text            as annee_mois,
    null::date            as date_debut_mois,
    null::date            as date_fin_mois,
    null::integer         as trimestre,
    null::text            as annee_trimestre,
    null::integer         as annee,
    null::date            as date_debut_trimestre,
    null::date            as date_fin_trimestre,
    null::date            as date_debut_annee,
    null::date            as date_fin_annee,
    null::boolean         as est_weekend,
    null::boolean         as est_fin_de_mois,
    null::boolean         as est_annee_bissextile
