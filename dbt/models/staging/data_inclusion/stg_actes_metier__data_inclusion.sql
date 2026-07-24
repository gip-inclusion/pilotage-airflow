select
    mois::date                    as mois,
    type_acte::text               as type_acte_raw,
    contexte_acte::text           as contexte_acte,
    reseaux_porteurs::text        as reseaux_porteurs,
    departement::text             as raw_departement,
    nombre_actes_metiers::integer as nombre_actes_metier
from {{ source('raw_actes_metier', 'stats_pdi_actes_metiers_data_inclusion') }}
