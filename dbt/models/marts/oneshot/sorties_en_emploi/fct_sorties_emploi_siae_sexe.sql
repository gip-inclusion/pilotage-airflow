with src as (
    select * from {{ ref('stg_dsn_siae_sexe') }}
)

select
    sortie_en_iae,
    mois_apres_sortie,
    type_siae,
    sexe,
    nombre_personnes_en_emploi,
    nombre_personnes_sans_emploi,
    taux_personnes_en_emploi,
    nombre_jours_median_travailles,
    nombre_contrats_median
from src
