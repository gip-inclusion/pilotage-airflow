select
    ft.id_salarie_asp,
    ft.id_salarie_emplois,
    ft.date_sortie,
    ft.type_structure,
    ft.motif_de_sortie,
    nir_corresp.nir,
    case when nir_hash is not null then 'oui' else 'non' end as ft
from {{ ref("xpnord_sorties_candidats_ft") }} as ft
left join {{ ref("nir_corresp") }} as nir_corresp
    on nir_corresp.hash_nir = ft.nir_ft
where ft.date_sortie is not null
union
select
    ft.id_salarie_asp,
    ft.id_salarie_emplois,
    ft.date_sortie,
    ft.type_structure,
    ft.motif_de_sortie,
    nir_corresp.nir
    case when nir_hash is not null then 'oui' else 'non' end as ft
from {{ ref("xpnord_sorties_candidats_asp") }} as ft
left join {{ ref("nir_corresp") }} as nir_corresp
    on nir_corresp.hash_nir = ft.nir_emplois
where ft.date_sortie is not null
