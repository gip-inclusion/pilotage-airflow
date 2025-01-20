with etp_conventionnes as (
    select
        (
            select round(sum("effectif_annuel_conventionné"))
            from {{ ref("suivi_etp_conventionnes_v2") }}
        ) as count_us,
        (
            select round(sum("ETP conventionnés"))
            from {{ source("DGEFP","dgefp_donnees_etp") }}
            where "Type Aide" = 'Aide au poste'
        ) as count_them
)

select *
from etp_conventionnes
where count_us != count_them
