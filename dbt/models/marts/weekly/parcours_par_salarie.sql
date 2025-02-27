select
	salarie.hash_nir,
	count(distinct pass_ag."hash_numéro_pass_iae") as nombre_pass,
	min(pass_ag.date_début) as date_début_premier_pass,
	max(pass_ag.date_fin) as date_fin_dernier_pass,
	case
		when max(pass_ag.date_fin) > CURRENT_DATE then 'oui'
		else 'non'
	end as personne_en_parcours,
	sum(pass_ag.durée) as somme_durées_tous_pass,
	extract(year from min(pass_ag.date_début)) as annee_début_premier_pass
from pass_agréments as pass_ag
left join "fluxIAE_Salarie" as salarie
on pass_ag.hash_numéro_pass_iae = salarie.hash_numéro_pass_iae
where pass_ag.type != 'Agrément PE'
group by salarie.hash_nir