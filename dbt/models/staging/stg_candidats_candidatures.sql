select
    {{ pilo_star(ref('candidats'),  relation_alias="c", except=["categorie_structure", "id_auteur_diagnostic_prescripteur", "id_auteur_diagnostic_employeur" ]) }},
    coalesce(c.id_auteur_diagnostic_prescripteur, c.id_auteur_diagnostic_employeur) as id_auteur_diagnostic,
    cd.id                                                                           as id_candidature,
    cd.genre_candidat,
    cd.date_embauche,
    cd.date_candidature,
    cd."état",
    cd.motif_de_refus,
    cd.type_structure,
    cd.categorie_structure,
    cd.id_structure,
    cd.origine,
    cd."origine_détaillée",
    cd.type_prescripteur
from {{ ref('candidats') }} as c
left join {{ ref('candidatures_echelle_locale') }} as cd
    on c.id = cd.id_candidat
