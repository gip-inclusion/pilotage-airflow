select
    debut_periode,
    fin_periode,
    mois1,
    mois2,
    type_utilisateur,
    type_organisation,
    departement,
    count(distinct case when premiere_visite_tous_tb = 'Oui' then true end) as nb_utilisateurs_acquis,
    sum(case when visites_mois1 > 0 and visites_mois2 > 0 then 1 end)       as nb_utilisateurs_revenus_periode,
    sum(case when visites_mois1 > 0 or visites_mois2 > 0 then 1 end)        as nb_utilisateurs_periode
from
    {{ ref("eph_visites_periodes_retention") }}
group by
    debut_periode,
    fin_periode,
    mois1,
    mois2,
    type_utilisateur,
    type_organisation,
    departement
order by
    debut_periode
