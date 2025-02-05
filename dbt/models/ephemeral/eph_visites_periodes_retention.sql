select
    mois1,
    mois2,
    debut_periode,
    fin_periode,
    id_utilisateur,
    type_utilisateur,
    type_organisation,
    departement,
    count(distinct case when premiere_visite_tous_tb = 'Oui' then id_utilisateur end)          as premiere_visite,
    count(distinct case when date_trunc('month', jour_visite) = mois1 then id_utilisateur end) as visites_mois1,
    count(distinct case when date_trunc('month', jour_visite) = mois2 then id_utilisateur end) as visites_mois2
from {{ ref('suivi_utilisateurs_tb_prive_semaine') }}
left join {{ ref('eph_periodes_retention') }}
    on mois1 = mois or mois2 = mois
group by
    mois1,
    mois2,
    debut_periode,
    fin_periode,
    id_utilisateur,
    type_utilisateur,
    type_organisation,
    departement
