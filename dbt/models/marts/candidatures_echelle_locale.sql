select
    {{ pilo_star(ref('stg_candidatures'), relation_alias='candidatures') }},
    case
        when candidatures.injection_ai = 0 then 'Non'
        else 'Oui'
    end                                   as reprise_de_stock_ai,
    candidats.sous_type_auteur_diagnostic as auteur_diag_candidat_detaille,
    candidats.type_auteur_diagnostic      as auteur_diag_candidat,
    candidats.eligibilite_dispositif,
    candidats.tranche_age,
    candidats.eligible_cej,
    candidats.sexe_selon_nir              as genre_candidat,
    candidats.eligible_cdi_inclusion,
    candidats.date_inscription            as date_inscription_candidat
from
    {{ ref('stg_candidatures') }} as candidatures
left join {{ ref('candidats') }} as candidats
    on candidats.id = candidatures.id_candidat
