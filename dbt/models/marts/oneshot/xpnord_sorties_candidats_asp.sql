select
    candidats.hash_nir            as nir_emplois,
    ft_candidats.nir_hash         as nir_ft,
    candidats.id_candidat_emplois as id_candidat_emplois,
    candidats.id_salarie_asp      as id_salarie_asp,
    sorties.date_sortie,
    sorties.type_structure,
    msct.motif_de_sortie
from {{ ref('candidats_pk') }} as candidats
left join {{ ref('fluxIAE_Salarie_v2') }} as salarie_asp
    on salarie_asp.salarie_id = candidats.id_salarie_asp
left join {{ source('emplois', 'pass_agréments') }} as pass
    on pass."hash_numéro_pass_iae" = candidats."hash_numéro_pass_iae"
-- pour filtrer les candidats non ft
left join {{ ref('ft_candidats_nord') }} as ft_candidats
    on ft_candidats.nir_hash = candidats.hash_nir
-- pour récupérer la sortie
left join {{ ref('sorties_definitives') }} as sorties
    on sorties.contrat_id_pph = candidats.id_salarie_asp
-- pour récupérer le motif de sortie
left join {{ ref('motif_sortie_contrat_termine') }} as msct
    on sorties.contrat_id_pph = msct.pph_id and sorties.date_sortie = msct.date_fin_reelle
-- on ne garde que les candidats :
-- - du nord
-- - un pass IAE a été attribué entre le 1 janvier 2021 et le 30 juin 2023.
where
    salarie_asp.salarie_code_dpt = '059'
    and pass."date_début" > '2021-01-01'
    and pass."date_début" < '2023-06-30'
    and sorties.date_sortie is not null
