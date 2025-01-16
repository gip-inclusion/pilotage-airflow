select
    af.af_numero_convention,
    af.af_numero_annexe_financiere,
    af.af_date_debut_effet_v2,
    af.af_date_fin_effet_v2,
    af.af_mesure_dispositif_code,
    af.af_montant_total_annuel,
    af.af_montant_unitaire_annuel_valeur,
    af.af_mt_cofinance,
    af.type_structure,
    af.type_structure_emplois,
    af.structure_id_siae,
    af.structure_denomination,
    af.structure_denomination_unique,
    af.commune_structure,
    af.code_insee_structure,
    af.siret_structure,
    af.nom_departement_structure,
    af.nom_region_structure,
    af.code_departement_af,
    af.nom_departement_af,
    af.nom_region_af,
    af.annee_af,
    af.year_diff,
    af.duree_annexe,
    af."effectif_annuel_conventionné",
    af."Montant_total_aide",
    af.part_conventionnement_cd,
    af.etp_conventionnes_cd,
    af.etp_conventionnes_etat,
    dgefp."SIRET actualisé",
    dgefp."Num Annexe",
    dgefp."Type Aide",
    dgefp."Financeur",
    dgefp."ETP conventionnés",
    dgefp."Montant conventionné",
    dgefp."ETP déclarés",
    dgefp."ETP réalisé plafonné au max des ETP conv",
    dgefp."Réalisation en ETP en deçà ou au-delà du conventionnement"
from {{ ref('suivi_etp_conventionnes_v2') }} as af
left join {{ source('DGEFP','dgefp_donnees_etp') }} as dgefp
    on
        af.annee_af = dgefp."Année d'analyse"
        and af.af_numero_annexe_financiere = dgefp."Num Annexe"
where
    "Type Aide" = 'Aide au poste'
    and af.annee_af in (select distinct dgefp."Année d'analyse" from {{ source('DGEFP','dgefp_donnees_etp') }})
