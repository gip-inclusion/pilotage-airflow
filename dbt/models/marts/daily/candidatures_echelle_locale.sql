select
    {{ pilo_star(ref('stg_candidatures'), except=['origine_détaillée', 'motif_de_refus'], relation_alias='candidatures') }},
    {{ translate_motif_refus('candidatures.motif_de_refus') }}
    as motif_de_refus,
    case
        when candidatures.injection_ai = 0 then 'Non'
        else 'Oui'
    end                                                 as reprise_de_stock_ai,
    case
        when candidatures.type_org_prescripteur = org.code
            then org.label
        else candidatures."origine_détaillée"
    end                                                 as "origine_détaillée",
    case
        when candidatures.temps_de_prise_en_compte <= 30 then '30 jours ou moins'
        when candidatures.temps_de_prise_en_compte > 30 and candidatures.temps_de_prise_en_compte <= 45 then '31 à 45 jours'
        when candidatures.temps_de_prise_en_compte > 45 and candidatures.temps_de_prise_en_compte <= 60 then '46 à 60 jours'
        when candidatures.temps_de_prise_en_compte > 60 and candidatures.temps_de_prise_en_compte <= 90 then '61 à 90 jours'
        when candidatures.temps_de_prise_en_compte > 90 then 'Plus de 90 jours'
    end                                                 as temps_de_prise_en_compte_intervalle,
    case
        when candidatures.temps_de_reponse <= 30 then '30 jours ou moins'
        when candidatures.temps_de_reponse > 30 and candidatures.temps_de_reponse <= 45 then '31 à 45 jours'
        when candidatures.temps_de_reponse > 45 and candidatures.temps_de_reponse <= 60 then '46 à 60 jours'
        when candidatures.temps_de_reponse > 60 and candidatures.temps_de_reponse <= 90 then '61 à 90 jours'
        when candidatures.temps_de_reponse > 90 then 'Plus de 90 jours'
    end                                                 as temps_de_reponse_intervalle,
    coalesce(candidats.tranche_age, 'Non renseigné')    as tranche_age,
    coalesce(candidats.sexe_selon_nir, 'Non renseigné') as genre_candidat,
    candidats.sous_type_auteur_diagnostic               as auteur_diag_candidat_detaille,
    candidats.type_auteur_diagnostic                    as auteur_diag_candidat,
    candidats.eligibilite_dispositif,
    candidats.eligible_cej,
    candidats.eligible_cdi_inclusion,
    candidats.date_inscription                          as date_inscription_candidat,
    orga."département"                                  as "département_orga"
from
    {{ ref('stg_candidatures') }} as candidatures
left join {{ ref('candidats') }} as candidats
    on candidats.id = candidatures.id_candidat
left join {{ ref('organisations') }} as orga
    on orga.id = candidatures.id_org_prescripteur
left join {{ source('emplois','c1_ref_type_prescripteur') }} as org
    on org.code = candidatures.type_org_prescripteur
