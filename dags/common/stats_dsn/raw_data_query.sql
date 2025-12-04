WITH base_data AS (
    SELECT DISTINCT
        raw_sal."nir_chiffré",
        CASE
            WHEN sal.salarie_rci_libelle = 'MME' THEN 'F'
            ELSE 'M'
        END AS sexe,
        strct.structure_siret_actualise AS siret,
        REPLACE(ctr.contrat_date_sortie_definitive::TEXT, '/', '')::TEXT AS mois_sortie,
        DATE_TRUNC('quarter', TO_DATE(ctr.contrat_date_sortie_definitive, 'DD/MM/YYYY')) AS trimestre,
        asp.type_structure_emplois,
        insee."ZE2020",
        -- real date used for deduplication
        TO_DATE(ctr.contrat_date_sortie_definitive, 'DD/MM/YYYY') AS date_sortie_definitive
    FROM "fluxIAE_Salarie_v2" AS sal
    INNER JOIN "fluxIAE_Salarie" AS raw_sal
        ON sal.salarie_id = raw_sal.salarie_id
    LEFT JOIN "fluxIAE_ContratMission_v2" AS ctr
        ON sal.salarie_id = ctr.contrat_id_pph
    LEFT JOIN "fluxIAE_Structure_v2" AS strct
        ON ctr.contrat_id_structure = strct.structure_id_siae
    LEFT JOIN "ref_mesure_dispositif_asp" AS asp
        ON ctr.contrat_mesure_disp_code = asp.af_mesure_dispositif_code
    LEFT JOIN "insee_zones_emploi" AS insee
        ON strct.zone_emploi_structure = insee."LIBZE2020"
    LEFT JOIN "fluxIAE_RefMotifSort_v2" AS rms
        ON ctr.contrat_motif_sortie_id = rms.rms_id
    LEFT JOIN "fluxIAE_RefCategorieSort_v2" AS rcs
        ON rms.rcs_id = rcs.rcs_id
    WHERE ctr.contrat_motif_sortie_id IS NOT NULL
      AND rcs.rcs_code != 5 -- Retrait des sorties constatées
      -- we want to remove the persons staying in the IAE
      AND rms.rms_code != 2   -- CDD dans une autre structure IAE
      AND rms.rms_code != 96  -- CDI Inclusion
      AND TO_DATE(ctr.contrat_date_sortie_definitive, 'DD/MM/YYYY')
            BETWEEN TO_DATE('${period_start}', 'YYYY/MM/DD')
                AND TO_DATE('${period_end}',   'YYYY/MM/DD')
),

-- keep only the latest exit per nir_chiffré
base_data_dedup AS (
    SELECT *
    FROM (
        SELECT
            b.*,
            ROW_NUMBER() OVER (
                PARTITION BY "nir_chiffré"
                ORDER BY date_sortie_definitive DESC
            ) AS rn
        FROM base_data b
    ) t
    WHERE rn = 1
),

counted_data AS (
    SELECT
        *,
        COUNT(*) OVER (
            PARTITION BY
                sexe, trimestre, type_structure_emplois, "ZE2020"
        ) AS nombre_lignes_similaires
    FROM base_data_dedup
)

SELECT
    100 AS code_ligne,
    NULL AS code_struct,
    "nir_chiffré",
    NULL AS nom,
    NULL AS prenom,
    NULL AS date_naissance,
    sexe,
    NULL AS siren,
    siret,
    mois_sortie,
    type_structure_emplois,
    "ZE2020",
    trimestre,
    NULL AS donnee_partenaire_4,
    NULL AS donnee_partenaire_5,
    NULL AS donnee_partenaire_6,
    NULL AS donnee_partenaire_7,
    NULL AS donnee_partenaire_8,
    NULL AS donnee_partenaire_9,
    NULL AS donnee_partenaire_10,
    NULL AS donnee_partenaire_11,
    NULL AS donnee_partenaire_12,
    NULL AS donnee_partenaire_13,
    NULL AS donnee_partenaire_14,
    NULL AS donnee_partenaire_15,
    NULL AS donnee_partenaire_16,
    NULL AS donnee_partenaire_17,
    NULL AS donnee_partenaire_18,
    NULL AS donnee_partenaire_19,
    NULL AS donnee_partenaire_20,
    nombre_lignes_similaires
FROM counted_data;
