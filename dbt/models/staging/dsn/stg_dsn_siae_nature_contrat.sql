with src_dsn as (
    select * from {{ source('dsn', 'dsn_type_siae_et_nature_contrat') }}
)

select
    sortie_en_iae,
    sorties_en_emploi,
    {{ months_after_exit('sorties_en_emploi') }} as mois_apres_sortie,
    type_de_siae                                              as type_siae,
    nature_de_contrat                                         as nature_contrat,
    nbre_personne_en_emploi::integer                          as nombre_personnes_en_emploi,
    nbre_personne_sans_emploi::integer                        as nombre_personnes_sans_emploi,
    taux_personnes_en_emploi::numeric                         as taux_personnes_en_emploi
from src_dsn
