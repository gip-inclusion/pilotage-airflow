with src as (
    select * from {{ ref('stg_dsn_geographie_secteur_activite') }}
)

select
    sortie_en_iae,
    mois_apres_sortie,
    code_zone_emploi,
    code_secteur_activite,
    nombre_personnes_en_emploi,
    nombre_personnes_sans_emploi,
    taux_personnes_en_emploi
from src
