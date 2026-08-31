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
        prestations.prestation_name_detail,
        prestations.prestation_category_fine,
        prestations.formation_name,
        prestations.is_technical_step,
        prestations.is_emploi_relevant,

        prestations.nb_files_active,
        prestations.preaccueil_sans_suite,

        prestations.sorties,
        prestations.sorties_avant_terme,
        prestations.sorties_terme,

        prestations.journees,
        prestations.journees_theoriques,

        prestations.direct_beneficiaires,
        prestations.direct_avec_orp_beneficiaires,
        prestations.direct_sans_orp_beneficiaires,
        prestations.direct_hors_murs_personnes,
        prestations.direct_hors_murs_journees,

        prestations.direct_hebergees_personnes,
        prestations.direct_hebergees_nuitees,

        prestations.suspensions_nb,

        prestations.direct_presentiel_total,
        prestations.direct_hybride_total,
        prestations.direct_distanciel_total,

        prestations.emploi_nb_repondants,
        prestations.emploi_acces_nb,
        prestations.emploi_presence_nb,
        prestations.emploi_acces_cdi,
        prestations.emploi_acces_cdd_plus6,
        prestations.emploi_acces_cdd_moins6,
        prestations.emploi_acces_alternance,
        prestations.emploi_acces_interim,
        prestations.emploi_acces_autre,

        prestations.preco_emploi_milieu_ordinaire,
        prestations.preco_entreprise_adaptee,
        prestations.preco_esat,
        prestations.preco_creation_entreprise,
        prestations.preco_maintien_emploi,
        prestations.preco_formation_droit_commun,
        prestations.preco_formation_alternance,
        prestations.preco_formation_esrp_dfa,
        prestations.preco_espo_specialisee_ueros,
        prestations.preco_service_accompagnement_social,
        prestations.preco_vie_sociale,
        prestations.preco_soins,
        prestations.preco_emploi_accompagne,
        prestations.preco_autres,
        prestations.preco_autres_precision,

        coalesce(prestations.emploi_acces_cdi, 0)
        + coalesce(prestations.emploi_acces_cdd_plus6, 0)                                       as emploi_durable_nb,

        coalesce(prestations.emploi_acces_cdd_moins6, 0)
        + coalesce(prestations.emploi_acces_alternance, 0)
        + coalesce(prestations.emploi_acces_interim, 0)
        + coalesce(prestations.emploi_acces_autre, 0)                                           as emploi_autre_nb,

        {{ safe_divide('prestations.emploi_acces_nb', 'prestations.emploi_nb_repondants') }}    as taux_acces_emploi,
        {{ safe_divide('prestations.emploi_presence_nb', 'prestations.emploi_nb_repondants') }} as taux_presence_emploi

    from prestations

    left join etablissements
        on prestations.uuid = etablissements.uuid

    where prestations.prestation_done is true

)

select *
from final
