select
    {{ pilo_star(source('emplois', 'candidatures'),
        except=["état", "motif_de_refus", "origine", "délai_de_réponse", "délai_prise_en_compte"]) }},
    case
        when "état" = 'Candidature déclinée' then 'Candidature refusée'
        else "état"
    end                                       as "état",
    case
        when motif_de_refus = 'Autre (détails dans le message ci-dessous)' then 'Motif "Autre" saisi sur les emplois'
        else motif_de_refus
    end                                       as motif_de_refus,
    case
        when origine = 'Candidat' then 'Candidature en ligne'
        else origine
    end                                       as origine,
    extract(day from "délai_de_réponse")      as temps_de_reponse,
    extract(day from "délai_prise_en_compte") as temps_de_prise_en_compte
from
    {{ source('emplois', 'candidatures') }}
