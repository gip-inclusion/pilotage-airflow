-- WIP!!
SELECT
	hash_nir,
	SPLIT_PART(STRING_AGG(af.nom_region_af, ',' ORDER BY ctr.contrat_date_embauche DESC), ',', 1) AS region,
	SPLIT_PART(STRING_AGG(af.nom_departement_af ',' ORDER BY ctr.contrat_date_embauche DESC), ',',1) AS departement,
	COUNT (DISTINCT contrat_id_structure) AS nombre_structures,
	COUNT (DISTINCT contrat_mesure_disp_code) AS nombre_type_dispositifs_differents,
	COUNT (DISTINCT (contrat_id_pph, groupe_contrat)) AS nombre_contrats_differents,
    -- ci-dessous il y a un problème car ils prends tous les types par contrat_id et pas par salarie_id, groupe_id
    -- j'ajouterait une colonne contrat_parent_id égale à contrat_id si contrat_type_contrat = 0 égale à tous les groupe_id
    -- je splitterais également tout ça pour pouvoir tester plus facilement différents bouts (à voir comment c'est votre pratique)
	STRING_AGG(ctr.contrat_mesure_disp_code, ',' ORDER BY ctr.contrat_date_embauche ) AS list_tous_type_dispositifs,
	-- ci-dessous si on fait distinct, on perd l'info chronologique car on peut avoir type1, type2, type2
    STRING_AGG(DISTINCT ctr.contrat_mesure_disp_code, ',' ) AS list_diff_type_dispositifs,
	SUM(
		CASE
			WHEN ctr.contrat_date_sortie_definitive IS NOT NULL THEN
				ctr.contrat_date_sortie_definitive - ctr.contrat_date_embauche
			ELSE CURRENT_DATE - ctr.contrat_date_embauche
		END
	) AS durée_tous_contrats
FROM stg_contrats AS ctr
LEFT JOIN (
	SELECT
		hash_nir,
		salarie_id
	FROM "fluxIAE_Salarie"
	GROUP BY hash_nir, salarie_id
) salarie
ON salarie.salarie_id = ctr.contrat_id_pph
LEFT JOIN "fluxIAE_AnnexeFinanciere_v2" as af
ON ctr.contrat_id_structure = af.af_id_structure AND ctr.contrat_mesure_disp_code = af.af_mesure_dispositif_code
WHERE af.af_etat_annexe_financiere_code IN ('VALIDE', 'PROVISOIRE', 'CLOTURE')
GROUP BY hash_nir