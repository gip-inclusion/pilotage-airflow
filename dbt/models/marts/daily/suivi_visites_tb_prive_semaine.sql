select
    visits.region                      as "région",
    visits.departement                 as "département_num",
    visits.type_utilisateur            as type_utilisateur,
    visits.type_organisation           as profil,
    visits.nom_tb,
    visits.semaine,
    visits.num_semaine,
    -- nb utilisateurs revenus plusieurs fois cette semaine
    nb_revenus.nb_utilisateurs_plusieurs_visites,
    -- mail des utilisateurs venus cette semaine
    array_agg(distinct c1_users.email) as liste_utilisateurs,
    -- nb total d'utilisateurs cette semaine
    count(visits.id_utilisateur)       as nb_utilisateurs
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
    visits.region,
    visits.departement,
    visits.type_utilisateur,
    visits.type_organisation,
    visits.num_semaine,
    visits.nom_tb,
    visits.semaine,
    nb_revenus.nb_utilisateurs_plusieurs_visites
