-- https://stats.inclusion.beta.gouv.fr/question/5426
Select
count(distinct recrutements.identifiant_contrat) as nombre_de_recrutements
From recrutements
where type_siae not in ('EIPA', 'ACIPA')
and {{annee_recrutement}}
and {{genre}}
and {{qpv}}
and {{zrr}}
and {{brsa}}
and {{rqth}}
and {{niveau_formation_salarie}}
and {{structure_denomination}}
and {{nom_departement_af}}
and {{nom_region_af}}
and {{type_siae}}
and {{tranche_age}}
and {{af_numero_annexe_financiere}}

