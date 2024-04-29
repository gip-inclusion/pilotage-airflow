select
    {{ pilo_star(source('emplois', 'candidatures'),
        except=["id", "date_mise_à_jour_metabase", "état", "motif_de_refus", "origine", "origine_détaillée"]) }},
    {{ pilo_star(ref('stg_organisations'),
        except=["id", "date_mise_à_jour_metabase", "ville", "code_commune", "type", "date_inscription", "total_candidatures", "total_membres", "total_embauches", "date_dernière_candidature"], relation_alias='org_prescripteur') }},
    candidatures.id                                        as id,
    struct.bassin_d_emploi                                 as bassin_emploi_structure,
    case
        when scvg.siret is not null and struct.type_struct = 'ACI' then 'Oui'
        else 'Non'
    end                                                    as structure_convergence,
    org_prescripteur.zone_emploi                           as bassin_emploi_prescripteur,
    org_prescripteur.type                                  as type_org_prescripteur,
    org_prescripteur.date_inscription                      as date_inscription_orga,
    grp_strct.groupe                                       as categorie_structure,
    case
        when candidatures."état" = 'Candidature déclinée' then 'Candidature refusée'
        else candidatures."état"
    end                                                    as "état",
    case
        when candidatures.motif_de_refus = 'Autre (détails dans le message ci-dessous)'
            then 'Motif "Autre" saisi sur les emplois'
        else candidatures.motif_de_refus
    end                                                    as motif_de_refus,
    case
        when candidatures.origine = 'Candidat' then 'Candidature en ligne'
        else candidatures.origine
    end                                                    as origine,
    case
        when candidatures."origine_détaillée" = 'Prescripteur habilité Autre'
            then 'Prescripteur habilité par habilitation préfectorale'
        else candidatures."origine_détaillée"
    end                                                    as "origine_détaillée",
    extract(day from candidatures."délai_de_réponse")      as temps_de_reponse,
    extract(day from candidatures."délai_prise_en_compte") as temps_de_prise_en_compte,
    extract(year from candidatures.date_candidature)       as annee_candidature
from
    {{ source('emplois', 'candidatures') }} as candidatures
left join {{ ref('stg_structures') }} as struct
    on struct.id = candidatures.id_structure
left join {{ ref('stg_organisations') }} as org_prescripteur
    on org_prescripteur.id = candidatures.id_org_prescripteur
left join
    {{ ref('groupes_structures') }} as grp_strct
    on grp_strct.structure = candidatures.type_structure
left join {{ ref('sirets_structures_convergence') }} as scvg
    on struct.siret = scvg.siret
