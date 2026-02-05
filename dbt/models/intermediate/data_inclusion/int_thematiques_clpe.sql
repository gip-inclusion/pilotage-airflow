with thematiques_per_service as (
    select
        id                                                                                                        as id_service,
        code_insee,
        max(case when thematique_principale like '%numerique%' then 1 else 0 end)                                 as numerique_thematique,
        max(case when thematique_principale like '%logement-hebergement%' then 1 else 0 end)                      as logement_hebergement_thematique,
        max(case when thematique_principale like '%sante%' then 1 else 0 end)                                     as sante_thematique,
        max(case when thematique_principale like '%mobilite%' then 1 else 0 end)                                  as mobilite_thematique,
        max(case when thematique_principale like '%lecture_ecriture_calcul%' then 1 else 0 end)                   as lecture_ecriture_calcul_thematique,
        max(case when thematique_principale like '%famille%' then 1 else 0 end)                                   as famille_thematique,
        max(case when thematique_principale like '%difficultes_administratives_ou_juridiques%' then 1 else 0 end) as difficultes_administratives_ou_juridiques_thematique,
        max(case when thematique_principale like '%difficultes_financieres%' then 1 else 0 end)                   as difficultes_financieres_thematique
    from {{ ref('stg_di_services') }},
        unnest(thematiques) as thematique_principale
    where thematiques is not null
    group by
        id,
        code_insee
)

select
    clpe.territoire_id,
    clpe.territoire_libelle,
    sum(di.numerique_thematique)                                 as nbr_thematiques_numeriques,
    sum(di.mobilite_thematique)                                  as nbr_thematiques_mobilite,
    sum(di.lecture_ecriture_calcul_thematique)                   as nbr_thematiques_lecture_ecriture_calcul,
    sum(di.famille_thematique)                                   as nbr_thematiques_famille,
    sum(di.difficultes_administratives_ou_juridiques_thematique) as nbr_thematiques_difficultes_administratives_ou_juridiques,
    sum(di.logement_hebergement_thematique)                      as nbr_thematiques_logement_hebergement,
    sum(di.difficultes_financieres_thematique)                   as nbr_thematiques_difficultes_financieres,
    sum(di.sante_thematique)                                     as nbr_thematiques_sante,
    sum(
        di.numerique_thematique
        + di.mobilite_thematique
        + di.lecture_ecriture_calcul_thematique
        + di.famille_thematique
        + di.difficultes_administratives_ou_juridiques_thematique
        + di.logement_hebergement_thematique
        + di.difficultes_financieres_thematique
        + di.sante_thematique
    )                                                            as nbr_total_thematiques
from thematiques_per_service as di
inner join {{ ref('ref_clpe_ft') }} as clpe
    on di.code_insee = clpe.commune_id
group by
    clpe.territoire_id,
    clpe.territoire_libelle
