select
    visites.mois,
    potentiel."région",
    potentiel."département",
    potentiel.type_utilisateur,
    potentiel.profil,
    visites.nb_organisations                       as nb_visites,
    potentiel.potentiel                            as potentiel,
    visites.nb_organisations / potentiel.potentiel as taux_couv
from {{ ref('nb_utilisateurs_potentiels') }} as potentiel
left join {{ ref('suivi_visites_tous_tb_prive_mensuel') }} as visites
    on
        visites.type_utilisateur = potentiel.type_utilisateur
        and visites.profil = potentiel.profil
        and visites."département" = potentiel."département"
        and visites."région" = potentiel."région"
group by
    visites.mois,
    potentiel."région",
    potentiel."département",
    potentiel.type_utilisateur,
    potentiel.profil,
    visites.nb_organisations,
    potentiel.potentiel
