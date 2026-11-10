-- Reproduit les structures exposées par l'API data·inclusion (api/src/data_inclusion/api/inclusion_data)
with structures as (

    select *
    from {{ ref('stg_di__structures') }}
    where not _is_closed and _is_valid

),

-- score qualité d'une structure = moyenne des scores de ses services exposés par l'API
scores as (

    select
        structures.id,
        coalesce(round(avg(services.score_qualite)::numeric, 2), 0)::double precision as score_qualite
    from structures
    left join {{ ref('stg_di__services') }} as services
        on
            structures.id = services.structure_id
            and services._is_valid
            -- règle de l'API, voir la doc de int_di__services
            and (services.code_insee is not null or not coalesce('en-presentiel' = any(services.modes_accueil), false))
    group by structures.id

),

doublons as (

    select
        structures.id,
        array_agg(json_build_object('source', autres.source, 'id', autres.id) order by autres.id) as doublons
    from structures
    inner join structures as autres
        on structures._cluster_id = autres._cluster_id and structures.id != autres.id
    group by structures.id

)

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
    scores.score_qualite,
    coalesce(doublons.doublons, '{}') as doublons,
    row_number() over (
        partition by coalesce(structures._cluster_id, structures.id)
        order by scores.score_qualite desc, structures.date_maj desc nulls last, structures.id asc
    ) = 1                             as est_structure_principale
from structures
inner join scores on structures.id = scores.id
left join doublons on structures.id = doublons.id
