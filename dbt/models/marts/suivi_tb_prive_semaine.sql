with follow_users as (
    select
        nom_tb,
        semaine,
        num_semaine,
        -- liste des utilisateurs venus cette semaine
        array_agg(id_utilisateur) as liste_utilisateurs,
        -- liste des utilisateurs venus la semaine dernière
        -- TODO (laurine) : a adapter pour récupérer les utilisateurs de toutes les semaines précédentes et non pas uniquement la precedente
        lag(array_agg(id_utilisateur)) over (partition by nom_tb order by num_semaine) as liste_utilisateurs_semaine_prec,
        -- nb total d'utilisateurs cette semaine
        count(id_utilisateur)     as nb_utilisateurs,
        -- nb d'utilisateurs revenus au moins une fois cette semaine
        count(
            case
                when nb_visites > 1 then 1
            end
        )                         as nb_utilisateurs_plusieurs_visites
    from {{ ref('suivi_utilisateurs_tb_prive_semaine') }}
    group by
        num_semaine,
        nom_tb,
        semaine
)

select
    *,
    -- visiteurs cumules
    array(select distinct u from unnest(array_cat(liste_utilisateurs, liste_utilisateurs_semaine_prec)) as u) as visiteurs_cumules,
    -- nouveaux visiteurs ie ne se sont jamais connectés avant cette semaine
    array(select distinct u from unnest(liste_utilisateurs) as u where u not in (select * from unnest(liste_utilisateurs_semaine_prec))) as nouveaux_visiteurs,
    -- anciens visiteurs ie se sont déjà connecté une fois avant cette semaine
    array(select distinct u from unnest(liste_utilisateurs) as u where u in (select * from unnest(liste_utilisateurs_semaine_prec))) as anciens_visiteurs
from follow_users
