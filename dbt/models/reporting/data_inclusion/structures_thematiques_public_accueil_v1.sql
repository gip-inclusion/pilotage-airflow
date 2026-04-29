select
    structures.source,
    structures.id,
    structures.nom,
    structures.date_maj,
    structures.description,
    structures.lien_source,
    structures.siret,
    structures.commune,
    structures.code_postal,
    structures.code_insee,
    structures.adresse,
    structures.complement_adresse,
    structures.longitude,
    structures.latitude,
    structures.telephone,
    structures.courriel,
    structures.site_web,
    structures.horaires_accueil,
    structures.accessibilite_lieu,
    structures.reseaux_porteurs,
    structures.adresse_certifiee,
    structures.score_qualite,
    structures.doublons,
    reseau_porteur,
    services.service_thematique,
    services.service_public,
    services.service_mode_accueil,
    structures.nom_region,
    structures.code_region_insee,
    structures.nom_departement,
    structures.code_departement_insee
from {{ ref('di_structures') }} as structures
left join lateral unnest(reseaux_porteurs) as reseau_porteur on true
left join {{ ref('services_thematiques_public_accueil_v1') }} as services
    on structures.id = services.structure_id
where structures.source != 'soliguide'
