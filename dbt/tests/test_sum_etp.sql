with sum_etp as (
    select
        (
            select round(sum(nombre_etp_consommes_reels_mensuels))
            from {{ ref("suivi_realisation_convention_mensuelle") }}
        ) as etp_somme_tous,
        (
            select round(sum(somme_etp_realises))
            from {{ ref("suivi_realisation_convention_par_structure") }}
        ) as etp_somme_strct
)

select *
from sum_etp
where etp_somme_tous != etp_somme_strct
