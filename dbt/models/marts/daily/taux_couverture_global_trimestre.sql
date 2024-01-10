select
    visites.trimestre,
    potentiel."région",
    potentiel."département_num",
    potentiel.type_utilisateur,
    potentiel.profil,
    visites.nb_organisations                                                     as nb_visites,
    potentiel.potentiel                                                          as potentiel,
    cast(visites.nb_organisations as float) / cast(potentiel.potentiel as float) as taux_couv
from {{ ref('nb_utilisateurs_potentiels') }} as potentiel
left join {{ ref('suivi_visites_tous_tb_prive_trimestre') }} as visites
    on
        visites.type_utilisateur = potentiel.type_utilisateur
        and visites.profil = potentiel.profil
        and visites."département_num" = potentiel."département_num"
        and visites."région" = potentiel."région"
group by
    visites.trimestre,
    potentiel."région",
    potentiel."département_num",
    potentiel.type_utilisateur,
    potentiel.profil,
    visites.nb_organisations,
    potentiel.potentiel
