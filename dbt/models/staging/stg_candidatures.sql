select
    {{ pilo_star(source('emplois', 'candidatures'),
        except=["date_mise_à_jour_metabase", "état", "motif_de_refus", "origine", "origine_détaillée", "candidature_archivee", "type_contrat"], relation_alias='candidatures') }},
    {{ pilo_star(ref('stg_organisations'),
        except=["id", "date_mise_à_jour_metabase", "ville", "code_commune", "type", "date_inscription", "total_candidatures", "total_membres", "total_embauches", "date_dernière_candidature"], relation_alias='org_prescripteur') }},
    c_type.label                                           as type_contrat_candidature,
    struct.nom_epci_structure,
    struct.bassin_d_emploi                                 as bassin_emploi_structure,
    org_prescripteur.zone_emploi                           as bassin_emploi_prescripteur,
    org_prescripteur.type                                  as type_org_prescripteur,
    org_prescripteur.date_inscription                      as date_inscription_orga,
    grp_strct.groupe                                       as categorie_structure,
    struct.structure_convergence,
    case
        when candidatures."état" = 'Candidature déclinée' then 'Candidature refusée'
        else candidatures."état"
    end                                                    as "état",
    {{ translate_motif_refus('candidatures.motif_de_refus') }},
    case
        when candidatures.origine_id_structure != candidatures.id_structure
            then 'Employeur orienteur'
        else candidatures.origine
    end                                                    as origine,
    case
        when candidatures."origine_détaillée" = 'Prescripteur habilité Autre'
            then 'Prescripteur habilité par habilitation préfectorale'
        else candidatures."origine_détaillée"
    end                                                    as "origine_détaillée",
    case
        when candidatures.candidature_archivee = 1 then 'Candidatures archivées'
        else 'Candidatures non archivées'
    end                                                    as candidature_archivee,
    extract(day from candidatures."délai_de_réponse")      as temps_de_reponse,
    extract(day from candidatures."délai_prise_en_compte") as temps_de_prise_en_compte,
    extract(year from candidatures.date_candidature)       as annee_candidature
from
    {{ source('emplois', 'candidatures') }} as candidatures
left join {{ source('emplois', 'c1_ref_type_contrat') }} as c_type
    on candidatures.type_contrat = c_type.code
left join {{ ref('stg_structures') }} as struct
    on candidatures.id_structure = struct.id
left join {{ ref('stg_organisations') }} as org_prescripteur
    on candidatures.id_org_prescripteur = org_prescripteur.id
left join
    {{ ref('groupes_structures') }} as grp_strct
    on candidatures.type_structure = grp_strct.structure
