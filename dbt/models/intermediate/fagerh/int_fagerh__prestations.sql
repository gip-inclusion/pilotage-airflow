with source as (

    select *
    from {{ ref('stg_fagerh__reponses') }}

),

prestations as (

    select
        source.uuid,
        source.id        as answer_id,
        prestation.key   as prestation_key,
        prestation.value as prestation_json
    from source
    cross join lateral jsonb_each(
        coalesce(nullif(source.prestations_json, ''), '{}')::jsonb
    ) as prestation (key, value)

),

final as (

    select
        uuid,
        answer_id,
        prestation_key,

        (prestation_json ->> 'done')::boolean                                                                         as prestation_done,
        nullif(prestation_json ->> 'fileActive', '')::integer                                                         as nb_files_active,

        nullif(prestation_json ->> 'preaccueilSansSuite', '')::integer                                                as preaccueil_sans_suite,
        nullif(prestation_json ->> 'sortiesAvantTerme', '')::integer                                                  as sorties_avant_terme,
        nullif(prestation_json ->> 'sortiesTerme', '')::integer                                                       as sorties_terme,
        nullif(prestation_json ->> 'sorties', '')::integer                                                            as sorties,
        nullif(prestation_json ->> 'journees', '')::integer                                                           as journees,
        nullif(prestation_json ->> 'journeesTheoriques', '')::integer                                                 as journees_theoriques,
        nullif(prestation_json #>> '{directAvecOrp,row,beneficiaires}', '')::integer                                  as direct_beneficiaires,

        nullif(prestation_json #>> '{directAvecOrp,row,discontinue_personnes}', '')::integer                          as direct_discontinue_personnes,
        nullif(prestation_json #>> '{directAvecOrp,row,presentiel_total}', '')::integer                               as direct_presentiel_total,
        nullif(prestation_json #>> '{directAvecOrp,row,presentiel_complet}', '')::integer                             as direct_presentiel_complet,
        nullif(prestation_json #>> '{directAvecOrp,row,presentiel_partiel}', '')::integer                             as direct_presentiel_partiel,
        nullif(prestation_json #>> '{directAvecOrp,row,hybride_total}', '')::integer                                  as direct_hybride_total,
        nullif(prestation_json #>> '{directAvecOrp,row,hybride_complet}', '')::integer                                as direct_hybride_complet,
        nullif(prestation_json #>> '{directAvecOrp,row,hybride_partiel}', '')::integer                                as direct_hybride_partiel,
        nullif(prestation_json #>> '{directAvecOrp,row,distanciel_total}', '')::integer                               as direct_distanciel_total,

        nullif(prestation_json #>> '{directAvecOrp,row,distanciel_complet}', '')::integer                             as direct_distanciel_complet,
        nullif(prestation_json #>> '{directAvecOrp,row,distanciel_partiel}', '')::integer                             as direct_distanciel_partiel,

        nullif(prestation_json #>> '{directAvecOrp,row,hors_murs_personnes}', '')::integer                            as direct_hors_murs_personnes,
        nullif(prestation_json #>> '{directAvecOrp,row,hors_murs_journees}', '')::integer                             as direct_hors_murs_journees,
        nullif(prestation_json #>> '{directAvecOrp,row,hebergees_personnes}', '')::integer                            as direct_hebergees_personnes,

        nullif(prestation_json #>> '{directAvecOrp,row,hebergees_journees}', '')::integer                             as direct_hebergees_journees,
        nullif(prestation_json #>> '{directAvecOrp,row,hebergees_nuitees}', '')::integer                              as direct_hebergees_nuitees,
        nullif(prestation_json #>> '{sortiesBloc,nb}', '')::integer                                                   as sorties_nb,
        nullif(prestation_json #>> '{sortiesBloc,raisons,depart_volontaire}', '')::integer                            as sorties_depart_volontaire,
        nullif(prestation_json #>> '{sortiesBloc,raisons,sante}', '')::integer                                        as sorties_sante,
        nullif(prestation_json #>> '{sortiesBloc,raisons,reorientation}', '')::integer                                as sorties_reorientation,
        nullif(prestation_json #>> '{sortiesBloc,raisons,disciplinaire}', '')::integer                                as sorties_disciplinaire,
        nullif(prestation_json #>> '{sortiesBloc,raisons,objectif}', '')::integer                                     as sorties_objectif,
        nullif(prestation_json #>> '{sortiesBloc,raisons,reprise_emploi}', '')::integer                               as sorties_reprise_emploi,
        nullif(prestation_json #>> '{sortiesBloc,raisons,je_ne_sais_pas}', '')::integer                               as sorties_je_ne_sais_pas,
        nullif(prestation_json #>> '{sortiesBloc,raisons,autres_nb}', '')::integer                                    as sorties_autres_nb,
        nullif(prestation_json #>> '{suspensionsBloc,nb}', '')::integer                                               as suspensions_nb,
        nullif(prestation_json #>> '{suspensionsBloc,raisons,sante_non_liee}', '')::integer                           as suspensions_sante_non_liee,
        nullif(prestation_json #>> '{suspensionsBloc,raisons,evolution_handicap}', '')::integer                       as suspensions_evolution_handicap,
        nullif(prestation_json #>> '{suspensionsBloc,raisons,evolution_sociale}', '')::integer                        as suspensions_evolution_sociale,
        nullif(prestation_json #>> '{suspensionsBloc,raisons,parcours_suspendu}', '')::integer                        as suspensions_parcours_suspendu,

        nullif(prestation_json #>> '{suspensionsBloc,raisons,autres_nb}', '')::integer                                as suspensions_autres_nb,
        nullif(prestation_json #>> '{preconisationsBloc,emploi_milieu_ordinaire}', '')::integer                       as preco_emploi_milieu_ordinaire,
        nullif(prestation_json #>> '{preconisationsBloc,entreprise_adaptee}', '')::integer                            as preco_entreprise_adaptee,
        nullif(prestation_json #>> '{preconisationsBloc,esat}', '')::integer                                          as preco_esat,
        nullif(prestation_json #>> '{preconisationsBloc,creation_entreprise}', '')::integer                           as preco_creation_entreprise,
        nullif(prestation_json #>> '{preconisationsBloc,maintien_emploi}', '')::integer                               as preco_maintien_emploi,
        nullif(prestation_json #>> '{preconisationsBloc,formation_droit_commun}', '')::integer                        as preco_formation_droit_commun,
        nullif(prestation_json #>> '{preconisationsBloc,formation_alternance}', '')::integer                          as preco_formation_alternance,
        nullif(prestation_json #>> '{preconisationsBloc,formation_esrp_dfa}', '')::integer                            as preco_formation_esrp_dfa,
        nullif(prestation_json #>> '{preconisationsBloc,espo_specialisee_ueros}', '')::integer                        as preco_espo_specialisee_ueros,

        nullif(prestation_json #>> '{preconisationsBloc,service_accompagnement_social}', '')::integer                 as preco_service_accompagnement_social,
        nullif(prestation_json #>> '{preconisationsBloc,vie_sociale}', '')::integer                                   as preco_vie_sociale,
        nullif(prestation_json #>> '{preconisationsBloc,soins}', '')::integer                                         as preco_soins,
        nullif(prestation_json #>> '{preconisationsBloc,emploi_accompagne}', '')::integer                             as preco_emploi_accompagne,
        nullif(prestation_json #>> '{preconisationsBloc,autres}', '')::integer                                        as preco_autres,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_sorties_n_1}', '')::integer                             as emploi_sorties_n_1,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_nb_repondants}', '')::integer                           as emploi_nb_repondants,

        nullif(prestation_json #>> '{directAvecOrp,row,emploi_acces_nb}', '')::integer                                as emploi_acces_nb,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_acces_cdi}', '')::integer                               as emploi_acces_cdi,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_acces_cdd_plus6}', '')::integer                         as emploi_acces_cdd_plus6,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_acces_cdd_moins6}', '')::integer                        as emploi_acces_cdd_moins6,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_acces_alternance}', '')::integer                        as emploi_acces_alternance,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_acces_interim}', '')::integer                           as emploi_acces_interim,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_acces_autre}', '')::integer                             as emploi_acces_autre,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_acces_encore_nb}', '')::integer                         as emploi_acces_encore_nb,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_sans_acces_formation_qualifiante_nb}', '')::integer     as emploi_sans_acces_formation_qualifiante_nb,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_sans_acces_formation_non_qualifiante_nb}', '')::integer as emploi_sans_acces_formation_non_qualifiante_nb,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_sans_acces_stage_nb}', '')::integer                     as emploi_sans_acces_stage_nb,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_sans_acces_benevolat_nb}', '')::integer                 as emploi_sans_acces_benevolat_nb,
        nullif(prestation_json #>> '{directAvecOrp,row,emploi_presence_nb}', '')::integer                             as emploi_presence_nb,
        prestation_json                                                                                               as json_prestation_raw,
        prestation_json -> 'visitedBlocks'                                                                            as visited_blocks,

        prestation_json -> 'coh' -> 'genre'                                                                           as json_coh_genre,
        prestation_json -> 'coh' -> 'age'                                                                             as json_coh_age,
        prestation_json -> 'coh' -> 'niveau_entree'                                                                   as json_coh_niveau_entree,
        prestation_json -> 'coh' -> 'situation_entree'                                                                as json_coh_situation_entree,
        prestation_json -> 'coh' -> 'ressources_entree'                                                               as json_coh_ressources_entree,
        prestation_json -> 'coh' -> 'pathologies'                                                                     as json_coh_pathologies,
        prestation_json -> 'coh' -> 'origine_handicap'                                                                as json_coh_origine_handicap,
        prestation_json -> 'coh' -> 'lesion_origine'                                                                  as json_coh_lesion_origine,
        prestation_json -> 'geoRows'                                                                                  as json_geo_rows,
        prestation_json -> 'handicapMatrix'                                                                           as json_handicap_matrix,
        nullif(prestation_json #>> '{directAvecOrp,enabled}', '')                                                     as direct_avec_orp_enabled,
        nullif(prestation_json #>> '{directSansOrp,enabled}', '')                                                     as direct_sans_orp_enabled,
        nullif(prestation_json #>> '{indirect,enabled}', '')                                                          as indirect_enabled,
        nullif(prestation_json #>> '{sortiesBloc,raisons,autres_precision}', '')                                      as sorties_autres_precision,
        nullif(prestation_json #>> '{suspensionsBloc,raisons,autres_precision}', '')                                  as suspensions_autres_precision,

        nullif(prestation_json #>> '{preconisationsBloc,autres_precision}', '')                                       as preco_autres_precision

    from prestations

)

select *
from final
