select
    visites.nom_tb,
    visites.trimestre,
    potentiel."région",
    potentiel."département_num",
    potentiel.type_utilisateur,
    potentiel.profil,
    visites.nb_organisations                       as nb_visites,
    potentiel.potentiel                            as potentiel,
    visites.nb_organisations / potentiel.potentiel as taux_couv,
    case
        when potentiel.type_utilisateur = 'institution' then (select count from stg_nombre_orga)
    end                                            as potentiel_orga_national
from {{ ref('nb_utilisateurs_potentiels') }} as potentiel
left join {{ ref('suivi_visites_tb_prive_trimestre') }} as visites
    on
        visites.type_utilisateur = potentiel.type_utilisateur
        and visites.profil = potentiel.profil
        and visites."département_num" = potentiel."département_num"
        and visites."région" = potentiel."région"
group by
    visites.nom_tb,
    visites.trimestre,
    potentiel."région",
    potentiel."département_num",
    potentiel.type_utilisateur,
    potentiel.profil,
    potentiel.potentiel,
    visites.nb_organisations
