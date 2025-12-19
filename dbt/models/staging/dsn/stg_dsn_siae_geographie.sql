with src_dsn as (
    select * from {{ source('dsn', 'dsn_type_siae_et_geographie') }}
)

select
    sortie_en_iae,
    sorties_en_emploi,
    {{ months_after_exit('sorties_en_emploi') }} as mois_apres_sortie,
    type_de_siae                                              as type_siae,
    zone_d_emploi::text                                       as code_zone_emploi,
    nbre_personne_en_emploi::integer                          as nombre_personnes_en_emploi,
    nbre_personne_sans_emploi::integer                        as nombre_personnes_sans_emploi,
    taux_personnes_en_emploi::numeric                         as taux_personnes_en_emploi,
    nbre_de_jours_median_travailles::numeric                  as nombre_jours_median_travailles,
    nbre_contrats_median::numeric                             as nombre_contrats_median
from src_dsn
