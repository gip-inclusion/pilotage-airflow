select
    visites.nom_tb,
    visites.semaine,
    potentiel."région",
    potentiel."département_num",
    potentiel.type_utilisateur,
    potentiel.profil,
    visites.nb_organisations                       as nb_visites,
    potentiel.potentiel                            as potentiel,
    visites.nb_organisations / potentiel.potentiel as taux_couv
from {{ ref('nb_utilisateurs_potentiels') }} as potentiel
left join {{ ref('suivi_visites_tb_prive_semaine') }} as visites
    on
        visites.type_utilisateur = potentiel.type_utilisateur
        and visites.profil = potentiel.profil
        and visites."département_num" = potentiel."département_num"
        and visites."région" = potentiel."région"
group by
    visites.nom_tb,
    visites.semaine,
    potentiel."région",
    potentiel."département_num",
    potentiel.type_utilisateur,
    potentiel.profil,
    potentiel.potentiel,
    visites.nb_organisations
