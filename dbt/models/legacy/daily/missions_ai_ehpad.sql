/*

Here is the SQL request used to build the custom missions_ai_ehpad table.

Full specification of this table can be found in our "Documentation ITOU METABASE [Master doc]" shared google sheet.

No direct link here for safety reasons.

For more explanations about the mission / mei / emi tables, see `populate_metabase_fluxiae.py`.

*/

with missions as (
    select
        *,
        case
            /* ILIKE means (case) Insensitive LIKE */
            when
                mission_descriptif ilike '%AIEHPAD%'
                or mission_descriptif ilike '%AI EHPAD%'
                or mission_descriptif ilike '%AI-EHPAD%'
                or mission_descriptif ilike '%AIEPADH%'
                or mission_descriptif ilike '%AIEPAHD%'
                or mission_descriptif ilike '%AIHEPAD%'
                then 'AIEHPAD'
            when
                mission_descriptif ilike '%AIRESTO%'
                or mission_descriptif ilike '%AI RESTO%'
                or mission_descriptif ilike '%AI-RESTO%'
                then 'AIRESTO'
            when
                mission_descriptif ilike '%ETTIRESTO%'
                or mission_descriptif ilike '%ETTI RESTO%'
                or mission_descriptif ilike '%ETTI-RESTO%'
                then 'ETTIRESTO'
            when mission_descriptif ilike '%AIPH%'
                then 'AIPH'
            else 'AUTRE'
        end as code_operation
    from {{ source('fluxIAE', 'fluxIAE_Missions') }}
),

commune_structure as (
    select distinct
        code_insee,
        latitude,
        longitude
    from
        /* TODO @defajait DROP ASAP - use codes_insee_vs_codes_postaux instead */
        {{ source('oneshot', 'commune_gps') }}
)

select
    m.code_operation,
    m.mission_id_ctr,
    m.mission_id_mis,
    mei.mei_dsm_id,
    m.mission_descriptif,
    m.mission_code_rome,
    emi.emi_esm_etat_code                              as etat_saisie,
    emi.emi_pph_id                                     as id_personne,
    mei.mei_nombre_heures                              as nombre_heures,
    emi.emi_nb_heures_facturees                        as nombre_heures_facturees,
    s.structure_siret_actualise                        as siret_structure,
    s.structure_denomination                           as nom_structure,
    s.structure_adresse_admin_cp                       as code_postal_structure,
    s.structure_adresse_admin_commune                  as ville_structure,
    s.code_dept_structure                              as departement_code_structure,
    s.nom_departement_structure                        as departement_structure,
    s.nom_region_structure                             as region_structure,
    s.structure_code_naf                               as code_naf_structure,
    commune_structure.latitude                         as latitude_structure,
    commune_structure.longitude                        as longitude_structure,
    to_date(m.mission_date_creation, 'DD/MM/YYYY')     as mission_date_creation,
    to_date(m.mission_date_modification, 'DD/MM/YYYY') as mission_date_modification,
    to_date(m.mission_date_debut, 'DD/MM/YYYY')        as mission_date_debut,
    to_date(m.mission_date_fin, 'DD/MM/YYYY')          as mission_date_fin,
    concat(
        r.code_rome, ' ', r.description_code_rome
    )                                                  as mission_code_rome_complet,
    case
        when emi.emi_sme_mois is null
            then null
        else
            /* 15th day of the month instead of 1st day to avoid GT/GTE mistakes by metabase end user */
            to_date(
                concat('15/', emi.emi_sme_mois, '/', emi.emi_sme_annee),
                'DD/MM/YYYY'
            )
    end                                                as mois,
    to_date(emi.emi_date_validation, 'DD/MM/YYYY')     as date_validation,
    trim(substr(
        cm.contrat_mesure_disp_code,
        1,
        char_length(cm.contrat_mesure_disp_code) - 3
    ))                                                 as type_structure
from
    /* TODO use lateral joins instead maybe */
    missions as m
left outer join {{ source('fluxIAE', 'fluxIAE_MissionsEtatMensuelIndiv') }} as mei
    on m.mission_id_mis = mei.mei_mis_id
left outer join {{ source('fluxIAE', 'fluxIAE_EtatMensuelIndiv') }} as emi
    on mei.mei_dsm_id = emi.emi_dsm_id
left outer join {{ source('fluxIAE', 'fluxIAE_ContratMission') }} as cm
    on m.mission_id_ctr = cm.contrat_id_ctr
left outer join {{ ref('fluxIAE_Structure_v2') }} as s
    on cm.contrat_id_structure = s.structure_id_siae
left outer join commune_structure
    on
        trim(cast(s.structure_adresse_admin_code_insee as varchar))
        = trim(cast(commune_structure.code_insee as varchar))
left outer join {{ source('emplois', 'codes_rome') }} as r
    on m.mission_code_rome = r.code_rome
where m.code_operation in ('AIEHPAD', 'AIPH', 'AIRESTO', 'ETTIRESTO')
