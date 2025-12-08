with sum_freins as (
    select
        clpe.territoire_id,
        clpe.territoire_libelle,
        clpe.departement_id,
        count(distinct id)                                       as nbr_demandeurs_emplois,
        sum(iop.numerique_frein)                                 as nbr_freins_numeriques,
        sum(iop.mobilite_frein)                                  as nbr_freins_mobilite,
        sum(iop.lecture_ecriture_calcul_frein)                   as nbr_freins_lecture_ecriture_calcul,
        sum(iop.famille_frein)                                   as nbr_freins_famille,
        sum(iop.difficultes_administratives_ou_juridiques_frein) as nbr_freins_difficultes_administratives_ou_juridiques,
        sum(iop.logement_hebergement_frein)                      as nbr_freins_logement_hebergement,
        sum(iop.difficultes_financieres_frein)                   as nbr_freins_difficultes_financieres,
        sum(iop.sante_frein)                                     as nbr_freins_sante,
        sum(
            iop.numerique_frein
            + iop.mobilite_frein
            + iop.lecture_ecriture_calcul_frein
            + iop.famille_frein
            + iop.difficultes_administratives_ou_juridiques_frein
            + iop.logement_hebergement_frein
            + iop.difficultes_financieres_frein
            + iop.sante_frein
        )                                                        as nbr_total_freins
    from {{ ref('stg_france_travail_iop') }} as iop
    left join {{ ref('ref_clpe_ft') }} as clpe
        on iop.code_insee = clpe.commune_id
    group by
        clpe.territoire_id,
        clpe.territoire_libelle,
        clpe.departement_id
)

select
    territoire_id,
    territoire_libelle,
    departement_id,
    nbr_demandeurs_emplois,
    frein_type.frein,
    frein_type.nombre_de_freins
from sum_freins,
    lateral (
        values
        ('numeriques', nbr_freins_numeriques),
        ('mobilite', nbr_freins_mobilite),
        ('lecture-ecriture-calcul', nbr_freins_lecture_ecriture_calcul),
        ('famille', nbr_freins_famille),
        ('difficultes-administratives-ou-juridiques', nbr_freins_difficultes_administratives_ou_juridiques),
        ('logement-hebergement', nbr_freins_logement_hebergement),
        ('difficultes-financieres', nbr_freins_difficultes_financieres),
        ('sante', nbr_freins_sante),
        ('total freins', nbr_total_freins)
    ) as frein_type (frein, nombre_de_freins)
where territoire_id is not null
order by territoire_id, frein
