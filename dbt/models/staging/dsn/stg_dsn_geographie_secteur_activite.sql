with src_dsn as (
    select *
    from {{ source('dsn', 'dsn_geographie_et_secteur_activite') }}
)

select
    sortie_en_iae,
    sorties_en_emploi,
    {{ months_after_exit('sorties_en_emploi') }} as mois_apres_sortie,
    zone_d_emploi::text                                       as code_zone_emploi,
    secteur_d_activite::text                                  as code_secteur_activite,
    nbre_personne_en_emploi::integer                          as nombre_personnes_en_emploi,
    nbre_personne_sans_emploi::integer                        as nombre_personnes_sans_emploi,
    taux_personnes_en_emploi::numeric                         as taux_personnes_en_emploi
from src_dsn
