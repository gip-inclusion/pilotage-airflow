select
    id,
    "id_anonymisé",
    id_candidat,
    "id_candidat_anonymisé",
    date_candidature,
    date_embauche,
    "délai_de_réponse",
    "délai_prise_en_compte",
    "département_structure",
    safir_org_prescripteur,
    id_structure                              as cdd_id_structure,
    id_org_prescripteur                       as cdd_id_org_prescripteur,
    motif_de_refus,
    "nom_département_structure",
    "région_structure",
    nom_structure,
    type_structure,
    "origine_détaillée",
    nom_org_prescripteur,
    "nom_prénom_conseiller",
    injection_ai,
    case
        when "état" = 'Candidature déclinée' then 'Candidature refusée'
        else "état"
    end                                       as "état",
    case
        when origine = 'Candidat' then 'Candidature en ligne'
        else origine
    end                                       as origine,
    extract(day from "délai_de_réponse")      as temps_de_reponse,
    extract(day from "délai_prise_en_compte") as temps_de_prise_en_compte
from
    {{ source('emplois', 'candidatures') }}
