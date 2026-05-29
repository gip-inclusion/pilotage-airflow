with services as (
    select *
    from {{ ref('stg_di__services') }}

)

select
    services.id,
    services.source,
    services.structure_id,

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

    services.code_insee as code_commune_insee,
    services.code_postal,
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
from services
where source != 'soliguide'
