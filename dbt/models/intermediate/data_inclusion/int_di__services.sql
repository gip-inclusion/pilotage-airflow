-- Reproduit les services exposés par l'API data·inclusion, voir la doc
select
    services.source,
    services.structure_id,
    services.id,
    services.nom,
    services.description,
    services.lien_source,
    services.date_maj,
    services.type,
    services.thematiques,
    services.frais,
    services.frais_precisions,
    services.publics,
    services.publics_precisions,
    services.conditions_acces,
    services.commune,
    services.code_postal,
    services.code_insee,
    services.adresse,
    services.complement_adresse,
    services.longitude,
    services.latitude,
    services.telephone,
    services.courriel,
    services.modes_accueil,
    services.zone_eligibilite,
    services.contact_nom_prenom,
    services.lien_mobilisation,
    services.modes_mobilisation,
    services.mobilisable_par,
    services.mobilisation_precisions,
    services.volume_horaire_hebdomadaire,
    services.nombre_semaines,
    services.horaires_accueil,
    services.adresse_certifiee,
    services.score_qualite
from {{ ref('stg_di__services') }} as services
inner join {{ ref('int_di__structures') }} as structures
    on services.structure_id = structures.id
where
    services._is_valid
    -- règle de l'API : un service en présentiel sans code commune ne peut pas être localisé
    and (services.code_insee is not null or not coalesce('en-presentiel' = any(services.modes_accueil), false))
