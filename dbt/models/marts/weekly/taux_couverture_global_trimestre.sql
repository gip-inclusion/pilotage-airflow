select
    visites.trimestre,
    potentiel."région",
    potentiel."département_num",
    potentiel.type_utilisateur,
    potentiel.profil,
    visites.nb_organisations                       as nb_visites,
    potentiel.potentiel,
    visites.nb_organisations / potentiel.potentiel as taux_couv,
    case
        when potentiel.type_utilisateur = 'institution' then (select count from stg_nombre_orga)
    end                                            as potentiel_orga_national
from {{ ref('nb_utilisateurs_potentiels') }} as potentiel
left join {{ ref('suivi_visites_tous_tb_prive_trimestre') }} as visites
    on
        potentiel.type_utilisateur = visites.type_utilisateur
        and potentiel.profil = visites.profil
        and potentiel."département_num" = visites."département_num"
        and potentiel."région" = visites."région"
group by
    visites.trimestre,
    potentiel."région",
    potentiel."département_num",
    potentiel.type_utilisateur,
    potentiel.profil,
    visites.nb_organisations,
    potentiel.potentiel
