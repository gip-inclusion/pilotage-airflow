with offre_insertion as (
    select
        {{ dbt_utils.star(ref('int_metriques_offre_insertion')) }},
        -- Indicateur de tension : besoin / (besoin + offre)
        {{ couverture('besoin_numerique', 'offre_numerique') }}                                                                 as couverture_numerique,
        {{ couverture('besoin_mobilite', 'offre_mobilite') }}                                                                   as couverture_mobilite,
        {{ couverture('besoin_famille', 'offre_famille') }}                                                                     as couverture_famille,
        {{ couverture('besoin_sante', 'offre_sante') }}                                                                         as couverture_sante,
        {{ couverture('besoin_lecture_ecriture_calcul', 'offre_lecture_ecriture_calcul') }}                                     as couverture_lecture_ecriture_calcul,
        {{ couverture('besoin_logement', 'offre_logement') }}                                                                   as couverture_logement,
        {{ couverture('besoin_difficultes_financieres', 'offre_difficultes_financieres') }}                                     as couverture_difficultes_financieres,
        {{ couverture('besoin_difficultes_administratives_ou_juridiques', 'offre_difficultes_administratives_ou_juridiques') }} as couverture_difficultes_administratives_ou_juridiques
    from {{ ref('int_metriques_offre_insertion') }}
)

select
    oi.id_clpe,
    oi.libelle_clpe,
    oi.nombre_demandeurs_emploi,
    oi.demandeurs_emploi_avec_freins,
    oi.nbr_total_thematiques,
    frein_type.type_frein,
    frein_type.nbr_de_par_frein,
    frein_type.nbr_thematique,
    frein_type.indice_couverture
from offre_insertion as oi
cross join lateral (
    values
    (
        'Numérique',
        oi.frein_numerique,
        oi.nbr_thematiques_numeriques,
        oi.couverture_numerique
    ),
    (
        'Mobilité',
        oi.frein_mobilite,
        oi.nbr_thematiques_mobilite,
        oi.couverture_mobilite
    ),
    (
        'Lecture/Écriture/Calcul',
        oi.frein_savoir,
        oi.nbr_thematiques_lecture_ecriture_calcul,
        oi.couverture_lecture_ecriture_calcul
    ),
    (
        'Famille',
        oi.frein_familial,
        oi.nbr_thematiques_famille,
        oi.couverture_famille
    ),
    (
        'Difficultés Administratives/Juridiques',
        oi.frein_admin_jur,
        oi.nbr_thematiques_difficultes_administratives_ou_juridiques,
        oi.couverture_difficultes_administratives_ou_juridiques
    ),
    (
        'Logement/Hébergement',
        oi.frein_logement,
        oi.nbr_thematiques_logement_hebergement,
        oi.couverture_logement
    ),
    (
        'Difficultés Financières',
        oi.frein_financier,
        oi.nbr_thematiques_difficultes_financieres,
        oi.couverture_difficultes_financieres
    ),
    (
        'Santé',
        oi.frein_sante,
        oi.nbr_thematiques_sante,
        oi.couverture_sante
    )
) as frein_type (
    type_frein,
    nbr_de_par_frein,
    nbr_thematique,
    indice_couverture
)
