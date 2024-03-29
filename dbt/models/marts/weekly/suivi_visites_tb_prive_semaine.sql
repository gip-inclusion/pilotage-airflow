select
    visits.semaine,
    visits.nom_tb,
    visits.region                                          as "région",
    visits.departement                                     as "département_num",
    visits.type_utilisateur                                as type_utilisateur,
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
left join {{ source('emplois', 'utilisateurs') }} as c1_users
    on c1_users.id = cast(visits.id_utilisateur as integer)
left join {{ ref('stg_nb_utilisateurs_revenus_semaine') }} as nb_revenus
    on
        nb_revenus.num_semaine = cast(visits.num_semaine as integer)
        and nb_revenus.nom_tb = visits.nom_tb
        and nb_revenus.region = visits.region
        and nb_revenus.departement = visits.departement
        and nb_revenus.type_utilisateur = visits.type_utilisateur
        and nb_revenus.type_organisation = visits.type_organisation
group by
    visits.semaine,
    visits.nom_tb,
    visits.region,
    visits.departement,
    visits.type_utilisateur,
    visits.type_organisation,
    nb_revenus.nb_utilisateurs_plusieurs_visites
