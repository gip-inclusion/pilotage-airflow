select
    visits.region                               as "région",
    visits.departement                          as "département_num",
    visits.type_utilisateur                     as type_utilisateur,
    visits.type_organisation                    as profil,
    date_trunc('quarter', visits.semaine)       as trimestre,
    -- organisations venues ce trimestre
    array_agg(distinct visits.nom_organisation) as liste_organisations,
    count(distinct visits.nom_organisation)     as nb_organisations,
    -- mail des utilisateurs venus ce trimestre
    array_agg(distinct c1_users.email)          as liste_utilisateurs,
    -- nb total d'utilisateurs ce trimestre
    count(distinct c1_users.email)              as nb_utilisateurs
from {{ ref('suivi_utilisateurs_tb_prive_semaine') }} as visits
left join {{ source('emplois', 'utilisateurs') }} as c1_users
    on c1_users.id = cast(visits.id_utilisateur as integer)
group by
    date_trunc('quarter', visits.semaine),
    visits.region,
    visits.departement,
    visits.type_utilisateur,
    visits.type_organisation
