select
    visits.semaine,
    visits.nom_tb,
    visits.region                                          as "région",
    visits.departement                                     as "département_num",
    visits.type_utilisateur,
    visits.type_organisation                               as profil,
    -- nb utilisateurs revenus plusieurs fois cette semaine
    nb_revenus.nb_utilisateurs_plusieurs_visites,
    -- organisations venues ce mois
    cast(count(distinct visits.nom_organisation) as float) as nb_organisations,
    array_agg(distinct visits.nom_organisation)            as liste_organisations,
    -- mail des utilisateurs venus cette semaine
    array_agg(distinct c1_users.email)                     as liste_utilisateurs,
    -- nb total d'utilisateurs cette semaine
    count(distinct c1_users.email)                         as nb_utilisateurs
from {{ ref('suivi_utilisateurs_tb_prive_semaine') }} as visits
left join {{ source('emplois', 'utilisateurs_v0') }} as c1_users
    on c1_users.id = cast(visits.id_utilisateur as integer)
left join {{ ref('stg_nb_utilisateurs_revenus_semaine') }} as nb_revenus
    on
        nb_revenus.num_semaine = cast(visits.num_semaine as integer)
        and visits.nom_tb = nb_revenus.nom_tb
        and visits.region = nb_revenus.region
        and visits.departement = nb_revenus.departement
        and visits.type_utilisateur = nb_revenus.type_utilisateur
        and visits.type_organisation = nb_revenus.type_organisation
group by
    visits.semaine,
    visits.nom_tb,
    visits.region,
    visits.departement,
    visits.type_utilisateur,
    visits.type_organisation,
    nb_revenus.nb_utilisateurs_plusieurs_visites
