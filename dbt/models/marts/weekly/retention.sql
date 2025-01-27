select
    visites.debut_periode,
    visites.fin_periode,
    visites.mois1,
    visites.mois2,
    type_utilisateur,
    type_organisation,
    departement,
    count(distinct case when premiere_visite_tous_tb = 'Oui' then true end) as nb_utilisateurs_acquis,
    sum(case when visites_mois1 > 0 and visites_mois2 > 0 then 1 end)       as nb_utilisateurs_revenus_periode,
    sum(case when visites_mois1 > 0 or visites_mois2 > 0 then 1 end)        as nb_utilisateurs_periode
--    array_agg(case when visites_mois1 > 0 and visites_mois2 > 0 then id_utilisateur end) as utilisateurs_revenus_periode,
--    array_agg(case when visites_mois1 > 0 or visites_mois2 > 0 then id_utilisateur end) as utilisateurs_periode
from
    {{ ref("eph_visites_periodes_retention") }} as visites
group by
    visites.debut_periode,
    visites.fin_periode,
    visites.mois1,
    visites.mois2,
    type_utilisateur,
    type_organisation,
    departement
order by visites.debut_periode
