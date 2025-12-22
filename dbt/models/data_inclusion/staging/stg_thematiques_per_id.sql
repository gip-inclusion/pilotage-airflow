select
    id,
    code_insee,
    max(case when thematique_principale like '%numerique%' then 1 else 0 end)                          as numerique_thematique,
    max(case when thematique_principale like '%logement-hebergement%' then 1 else 0 end)               as logement_hebergement_thematique,
    max(case when thematique_principale like '%sante%' then 1 else 0 end)                              as sante_thematique,
    max(case when thematique_principale like '%mobilite%' then 1 else 0 end)                           as mobilite_thematique,
    max(case when thematique_principale like '%lecture_ecriture_calcul%' then 1 else 0 end)            as lecture_ecriture_calcul_thematique,
    max(case when thematique_principale like '%famille%' then 1 else 0 end)                            as famille_thematique,
    max(case when thematique_principale like '%difficultes_administratives_ou_juridiques%' then 1 end) as difficultes_administratives_ou_juridiques_thematique,
    max(case when thematique_principale like '%difficultes_financieres%' then 1 else 0 end)            as difficultes_financieres_thematique
from {{ source('data_inclusion', 'services_v1') }},
    unnest(thematiques) as thematique_principale
where thematiques is not null
group by
    id,
    code_insee
