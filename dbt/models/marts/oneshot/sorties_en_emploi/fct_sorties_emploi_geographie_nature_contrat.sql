with src as (
    select * from {{ ref('stg_dsn_geographie_nature_contrat') }}
)

select
    sortie_en_iae,
    mois_apres_sortie,
    code_zone_emploi,
    nature_contrat,
    nombre_personnes_en_emploi,
    nombre_personnes_sans_emploi,
    taux_personnes_en_emploi
from src
