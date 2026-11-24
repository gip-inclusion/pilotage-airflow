with cas_attendus (
    date_jour, annee_semaine_iso, jour_semaine_iso, nom_jour, nom_mois,
    date_debut_semaine, date_fin_semaine, date_fin_mois, est_weekend, est_annee_bissextile
) as (
    values
    (
        date '1990-01-01', '1990-W01', 1, 'lundi', 'janvier',
        date '1990-01-01', date '1990-01-07', date '1990-01-31', false, false
    ),
    (
        date '1999-12-31', '1999-W52', 5, 'vendredi', 'décembre',
        date '1999-12-27', date '2000-01-02', date '1999-12-31', false, false
    ),
    (
        date '2000-02-29', '2000-W09', 2, 'mardi', 'février',
        date '2000-02-28', date '2000-03-05', date '2000-02-29', false, true
    ),
    (
        date '2015-12-31', '2015-W53', 4, 'jeudi', 'décembre',
        date '2015-12-28', date '2016-01-03', date '2015-12-31', false, false
    ),
    (
        date '2016-01-01', '2015-W53', 5, 'vendredi', 'janvier',
        date '2015-12-28', date '2016-01-03', date '2016-01-31', false, true
    ),
    (
        date '2018-12-31', '2019-W01', 1, 'lundi', 'décembre',
        date '2018-12-31', date '2019-01-06', date '2018-12-31', false, false
    ),
    (
        date '2021-01-01', '2020-W53', 5, 'vendredi', 'janvier',
        date '2020-12-28', date '2021-01-03', date '2021-01-31', false, false
    ),
    (
        date '2023-04-30', '2023-W17', 7, 'dimanche', 'avril',
        date '2023-04-24', date '2023-04-30', date '2023-04-30', true, false
    ),
    (
        date '2024-02-29', '2024-W09', 4, 'jeudi', 'février',
        date '2024-02-26', date '2024-03-03', date '2024-02-29', false, true
    ),
    (
        date '2025-02-28', '2025-W09', 5, 'vendredi', 'février',
        date '2025-02-24', date '2025-03-02', date '2025-02-28', false, false
    ),
    (
        date '2035-12-31', '2036-W01', 1, 'lundi', 'décembre',
        date '2035-12-31', date '2036-01-06', date '2035-12-31', false, false
    ),
    (
        date '2045-12-31', '2045-W52', 7, 'dimanche', 'décembre',
        date '2045-12-25', date '2045-12-31', date '2045-12-31', true, false
    )
)

select cas_attendus.date_jour
from cas_attendus
left join {{ ref('dim_date') }} as dimension
    on cas_attendus.date_jour = dimension.date_jour
where
    dimension.date_id is null
    or dimension.annee_semaine_iso is distinct from cas_attendus.annee_semaine_iso
    or dimension.jour_semaine_iso is distinct from cas_attendus.jour_semaine_iso
    or dimension.nom_jour is distinct from cas_attendus.nom_jour
    or dimension.nom_mois is distinct from cas_attendus.nom_mois
    or dimension.date_debut_semaine is distinct from cas_attendus.date_debut_semaine
    or dimension.date_fin_semaine is distinct from cas_attendus.date_fin_semaine
    or dimension.date_fin_mois is distinct from cas_attendus.date_fin_mois
    or dimension.est_weekend is distinct from cas_attendus.est_weekend
    or dimension.est_annee_bissextile is distinct from cas_attendus.est_annee_bissextile
    or dimension.est_fin_de_mois is distinct from (cas_attendus.date_jour = cas_attendus.date_fin_mois)
