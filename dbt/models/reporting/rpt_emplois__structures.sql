with siae as (
    select *
    from {{ ref('dim_emplois__etablissements') }}
    where type_structure in ('ACI', 'AI', 'EI', 'EITI', 'ETTI')
),

communes as (
    select *
    from {{ ref('dim_commune') }}
)

select
    siae.id,
    siae.id_structure,
    siae.nom_structure,
    siae.type_structure,
    siae.siret,
    siae.code_naf,
    siae.source,
    siae.date_inscription_emplois,
    siae.active,
    siae.code_commune,
    communes.nom_commune,
    communes.code_departement_insee,
    communes.nom_departement,
    communes.nom_departement_complet,
    communes.nom_region,
    communes.nom_arrondissement,
    communes.nom_epci,
    communes.nom_zone_emploi,
    communes.code_ft_clpe,
    communes.nom_clpe
from siae
inner join communes
    on siae.code_commune = communes.code_commune_insee
