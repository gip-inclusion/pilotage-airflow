select
    visites.semaine,
    visites.type_utilisateur,
    visites.profil,
    visites.nom_tb,
    visites."département_num",
    visites."région",
    visites.nb_utilisateurs,
    potentiel.potentiel,
    case
        when potentiel.potentiel = 0 then null
        else visites.nb_utilisateurs::float / potentiel.potentiel::float
    end as taux_couverture
from {{ ref('suivi_visites_tb_prive_semaine') }} as visites
left join {{ ref('nb_utilisateurs_potentiels') }} as potentiel
    on
        visites.type_utilisateur = potentiel.type_utilisateur
        and visites.profil = potentiel.profil
        and visites."département_num" = potentiel."département_num"
        and visites."région" = potentiel."région"
group by
    visites.semaine,
    visites.type_utilisateur,
    visites.profil,
    visites.nom_tb,
    visites."département_num",
    visites."région",
    visites.nb_utilisateurs,
    potentiel.potentiel
