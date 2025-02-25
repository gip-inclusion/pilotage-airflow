SELECT
	salarie.hash_nir,
	COUNT(DISTINCT pass_ag.hash_numéro_pass_iae) AS nombre_pass,
	MIN(pass_ag.date_début) AS date_début_premier_pass,
	MAX(pass_ag.date_fin) AS date_fin_dernier_pass,
	CASE
		WHEN MAX(pass_ag.date_fin) > CURRENT_DATE THEN 'oui'
		ELSE 'non'
	END AS personne_en_parcours,
	SUM(pass_ag.durée) AS somme_durées_tous_pass,
	EXTRACT(YEAR FROM MIN(pass_ag.date_début)) AS annee_début_premier_pass
FROM pass_agréments AS pass_ag
LEFT JOIN "fluxIAE_Salarie" AS salarie
ON pass_ag.hash_numéro_pass_iae = salarie.hash_numéro_pass_iae
WHERE pass_ag.type != 'Agrément PE'
GROUP BY salarie.hash_nir