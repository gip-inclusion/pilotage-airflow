select
    visites.nom_tb,
    visites.semaine,
    potentiel."région",
    potentiel."département_num",
    potentiel.type_utilisateur,
    potentiel.profil,
    visites.nb_organisations                       as nb_visites,
    potentiel.potentiel,
    visites.nb_organisations / potentiel.potentiel as taux_couv
from {{ ref('nb_utilisateurs_potentiels') }} as potentiel
left join {{ ref('suivi_visites_tb_prive_semaine') }} as visites
    on
        potentiel.type_utilisateur = visites.type_utilisateur
        and potentiel.profil = visites.profil
        and potentiel."département_num" = visites."département_num"
        and potentiel."région" = visites."région"
group by
    visites.nom_tb,
    visites.semaine,
    potentiel."région",
    potentiel."département_num",
    potentiel.type_utilisateur,
    potentiel.profil,
    potentiel.potentiel,
    visites.nb_organisations
