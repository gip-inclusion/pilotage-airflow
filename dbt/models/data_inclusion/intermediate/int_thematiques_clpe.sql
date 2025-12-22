with sum_services as (
    select
        clpe.territoire_id,
        clpe.territoire_libelle,
        sum(di.numerique_thematique)                                 as nbr_services_numeriques,
        sum(di.mobilite_thematique)                                  as nbr_services_mobilite,
        sum(di.lecture_ecriture_calcul_thematique)                   as nbr_services_lecture_ecriture_calcul,
        sum(di.famille_thematique)                                   as nbr_services_famille,
        sum(di.difficultes_administratives_ou_juridiques_thematique) as nbr_services_difficultes_administratives_ou_juridiques,
        sum(di.logement_hebergement_thematique)                      as nbr_services_logement_hebergement,
        sum(di.difficultes_financieres_thematique)                   as nbr_services_difficultes_financieres,
        sum(di.sante_thematique)                                     as nbr_services_sante,
        sum(
            di.numerique_thematique
            + di.mobilite_thematique
            + di.lecture_ecriture_calcul_thematique
            + di.famille_thematique
            + di.difficultes_administratives_ou_juridiques_thematique
            + di.logement_hebergement_thematique
            + di.difficultes_financieres_thematique
            + di.sante_thematique
        )                                                            as nbr_total_services
    from {{ ref('stg_thematiques_per_id') }} as di
    left join {{ ref('ref_clpe_ft') }} as clpe
        on di.code_insee = clpe.commune_id
    group by
        clpe.territoire_id,
        clpe.territoire_libelle
)

select
    territoire_id,
    territoire_libelle,
    frein_type.services,
    frein_type.nombre_de_services
from sum_services,
    lateral (
        values
        ('numeriques', nbr_services_numeriques),
        ('mobilite', nbr_services_mobilite),
        ('lecture-ecriture-calcul', nbr_services_lecture_ecriture_calcul),
        ('famille', nbr_services_famille),
        ('difficultes-administratives-ou-juridiques', nbr_services_difficultes_administratives_ou_juridiques),
        ('logement-hebergement', nbr_services_logement_hebergement),
        ('difficultes-financieres', nbr_services_difficultes_financieres),
        ('sante', nbr_services_sante),
        ('total freins', nbr_total_services)
    ) as frein_type (services, nombre_de_services)
where territoire_id is not null
order by territoire_id, services
