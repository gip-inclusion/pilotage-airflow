with freins_clpe as (
    select
        id_clpe,
        libelle_clpe,
        date_extraction,
        sum(nombre_demandeurs_emploi)      as nombre_demandeurs_emploi,
        sum(frein_numerique)               as frein_numerique,
        sum(frein_mobilite)                as frein_mobilite,
        sum(frein_familial)                as frein_familial,
        sum(frein_sante)                   as frein_sante,
        sum(frein_savoir)                  as frein_savoir,
        sum(frein_logement)                as frein_logement,
        sum(frein_financier)               as frein_financier,
        sum(frein_admin_jur)               as frein_admin_jur,
        sum(demandeurs_emploi_avec_freins) as demandeurs_emploi_avec_freins
    from {{ ref("int_nombre_freins_par_groupe") }}
    group by
        id_clpe,
        libelle_clpe,
        date_extraction
)

select
    f.*,
    {{ dbt_utils.star(ref('int_thematiques_clpe')) }},
    {{ safe_divide('frein_numerique', 'demandeurs_emploi_avec_freins') }}                                   as besoin_numerique,
    {{ safe_divide('frein_mobilite', 'demandeurs_emploi_avec_freins') }}                                    as besoin_mobilite,
    {{ safe_divide('frein_familial', 'demandeurs_emploi_avec_freins') }}                                    as besoin_famille,
    {{ safe_divide('frein_sante', 'demandeurs_emploi_avec_freins') }}                                       as besoin_sante,
    {{ safe_divide('frein_savoir', 'demandeurs_emploi_avec_freins') }}                                      as besoin_lecture_ecriture_calcul,
    {{ safe_divide('frein_logement', 'demandeurs_emploi_avec_freins') }}                                    as besoin_logement,
    {{ safe_divide('frein_financier', 'demandeurs_emploi_avec_freins') }}                                   as besoin_difficultes_financieres,
    {{ safe_divide('frein_admin_jur', 'demandeurs_emploi_avec_freins') }}                                   as besoin_difficultes_administratives_ou_juridiques,
    {{ safe_divide('nbr_thematiques_numeriques', 'nbr_total_thematiques') }}                                as offre_numerique,
    {{ safe_divide('nbr_thematiques_mobilite', 'nbr_total_thematiques') }}                                  as offre_mobilite,
    {{ safe_divide('nbr_thematiques_lecture_ecriture_calcul', 'nbr_total_thematiques') }}                   as offre_lecture_ecriture_calcul,
    {{ safe_divide('nbr_thematiques_famille', 'nbr_total_thematiques') }}                                   as offre_famille,
    {{ safe_divide('nbr_thematiques_logement_hebergement', 'nbr_total_thematiques') }}                      as offre_logement,
    {{ safe_divide('nbr_thematiques_difficultes_administratives_ou_juridiques', 'nbr_total_thematiques') }} as offre_difficultes_administratives_ou_juridiques,
    {{ safe_divide('nbr_thematiques_difficultes_financieres', 'nbr_total_thematiques') }}                   as offre_difficultes_financieres,
    {{ safe_divide('nbr_thematiques_sante', 'nbr_total_thematiques') }}                                     as offre_sante
from freins_clpe as f
left join {{ ref('int_thematiques_clpe') }} as t
    on f.id_clpe = t.territoire_id
