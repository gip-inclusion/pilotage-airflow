with base_data as (
	select distinct
		sal."nir_chiffré",
		case
			when sal.salarie_rci_libelle = 'MME' then 'F'
			else 'M'
		end as sexe,
		strct.structure_siret_actualise as siret,
		replace(ctr.contrat_date_sortie_definitive::TEXT, '/','')::TEXT as mois_sortie,
		extract(QUARTER from to_date(ctr.contrat_date_sortie_definitive, 'DD/MM/YYYY')) as trimestre,
		asp.type_structure_emplois,
		insee."ZE2020"
	from "fluxIAE_Salarie_v2" as sal
	left join "fluxIAE_ContratMission_v2" as ctr
		on sal.salarie_id = ctr.contrat_id_pph
	left join "fluxIAE_Structure_v2" as strct
		on ctr.contrat_id_structure = strct.structure_id_siae
	left join "ref_mesure_dispositif_asp" as asp
		on ctr.contrat_mesure_disp_code = asp.af_mesure_dispositif_code
	left join "insee_zones_emploi" as insee
		on strct.zone_emploi_structure = insee."LIBZE2020"
	left join "fluxIAE_RefMotifSort_v2" as rms
		on ctr.contrat_motif_sortie_id = rms.rms_id
	left join "fluxIAE_RefCategorieSort_v2" as rcs
		on rms.rcs_id = rcs.rcs_id
	where ctr.contrat_motif_sortie_id is not null
	and rcs.rcs_libelle != 'Retrait des sorties constatées'
	and to_date(ctr.contrat_date_sortie_definitive, 'DD/MM/YYYY')
		between to_date('${period_start}', 'YYYY/MM/DD') and to_date('${period_end}', 'YYYY/MM/DD')
),

counted_data as (
	SELECT *,
        COUNT(*) OVER (
            PARTITION BY
                sexe, trimestre, type_structure_emplois, "ZE2020"
        ) AS nombre_lignes_similaires
    FROM base_data
)

select
    100 as code_ligne,
    null as code_struct,
    "nir_chiffré",
    null as nom,
    null as prenom,
    null as date_naissance,
    sexe,
    null as siren,
    siret,
    mois_sortie,
    type_structure_emplois,
    "ZE2020",
    trimestre,
    null as donnee_partenaire_4,
    null as donnee_partenaire_5,
    null as donnee_partenaire_6,
    null as donnee_partenaire_7,
    null as donnee_partenaire_8,
    null as donnee_partenaire_9,
    null as donnee_partenaire_10,
    null as donnee_partenaire_11,
    null as donnee_partenaire_12,
    null as donnee_partenaire_13,
    null as donnee_partenaire_14,
    null as donnee_partenaire_15,
    null as donnee_partenaire_16,
    null as donnee_partenaire_17,
    null as donnee_partenaire_18,
    null as donnee_partenaire_19,
    null as donnee_partenaire_20,
    nombre_lignes_similaires
from counted_data
