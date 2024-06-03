select
    candidats.hash_nir            as nir_emplois,
    ft_candidats.nir_hash         as nir_ft,
    candidats.id_candidat_emplois as id_candidat_emplois,
    candidats.id_salarie_asp      as id_salarie_asp,
    sorties.date_sortie,
    sorties.type_structure,
    msct.motif_de_sortie
from {{ source('oneshot', 'ft_iae_nord') }} as ft_candidats
left join {{ ref('candidats_pk') }} as candidats
    on candidats.hash_nir = ft_candidats.nir_hash
-- ici on croise avec les sorties définitives
-- duplication des candidats parceque certains candidats ont plusieurs sorties
left join {{ ref('sorties_definitives') }} as sorties
    on sorties.contrat_id_pph = candidats.id_salarie_asp
-- pour récupérer le motif de sortie
left join {{ ref('motif_sortie_contrat_termine') }} as msct
    on sorties.contrat_id_pph = msct.pph_id and sorties.date_sortie = msct.date_fin_reelle
