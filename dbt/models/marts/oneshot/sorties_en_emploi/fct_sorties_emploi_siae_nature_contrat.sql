with src as (
    select * from {{ ref('stg_dsn_siae_nature_contrat') }}
)

select
    sortie_en_iae,
    mois_apres_sortie,
    type_siae,
    nature_contrat,
    nombre_personnes_en_emploi,
    nombre_personnes_sans_emploi,
    taux_personnes_en_emploi
from src
