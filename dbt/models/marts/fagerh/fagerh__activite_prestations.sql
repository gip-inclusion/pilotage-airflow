with prestations as (

    select *
    from {{ ref('int_fagerh__prestations') }}

),

etablissements as (

    select *
    from {{ ref('int_fagerh__etablissements') }}

),

final as (

    select
        prestations.uuid,

        etablissements.finess,
        etablissements.establishment_name,
        etablissements.departement,

        prestations.prestation_key_base,
        prestations.prestation_group,
        prestations.prestation_label,
        prestations.orp_status,
        prestations.is_reliable_prestation_mapping,
        prestations.is_unmapped_prestation,

        prestations.nb_files_active,
        prestations.preaccueil_sans_suite,

        prestations.sorties,
        prestations.sorties_avant_terme,
        prestations.sorties_terme,

        prestations.journees,
        prestations.journees_theoriques,

        prestations.direct_beneficiaires,
        prestations.direct_hors_murs_personnes,
        prestations.direct_hors_murs_journees,

        prestations.direct_hebergees_personnes,
        prestations.direct_hebergees_nuitees,

        prestations.suspensions_nb,

        prestations.direct_presentiel_total,
        prestations.direct_hybride_total,
        prestations.direct_distanciel_total

    from prestations

    left join etablissements
        on prestations.uuid = etablissements.uuid

    where prestations.prestation_done is true

)

select *
from final
