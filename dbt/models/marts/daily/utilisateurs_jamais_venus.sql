select
    utilisateurs_all.email,
    utilisateurs_all.type,
    case
        when array_agg(nom_tb) = array[null] then null
        else array_agg(nom_tb)
    end as tb_visited,
    case
        when array_agg(nom_tb) = array[null] then 0
        else array_length(array_agg(nom_tb), 1)
    end as nb_tb_visited
from
    {{ source('emplois', 'utilisateurs_v0') }} as utilisateurs_all
left join
    {{ ref('suivi_utilisateurs_tb_prive_semaine') }} as utilisateurs_tb_prives
    on utilisateurs_all.email = utilisateurs_tb_prives.email_utilisateur
group by
    utilisateurs_all.email,
    utilisateurs_all.type
