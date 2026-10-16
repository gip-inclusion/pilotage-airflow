with candidats as (
    select
        id          as id_candidat,
        code_commune_insee,
        departement as departement_source
    from {{ ref('stg_emplois__candidats') }}
),

communes as (
    select
        code_commune_insee,
        code_departement_insee as departement_commune
    from {{ ref('dim_commune') }}
)

select
    candidats.id_candidat,
    candidats.code_commune_insee,
    candidats.departement_source,
    communes.departement_commune,
    coalesce(
        nullif(btrim(communes.departement_commune), ''),
        nullif(btrim(candidats.departement_source), '')
    )   as departement,
    case
        when nullif(btrim(communes.departement_commune), '') is not null then 'dim_commune'
        when nullif(btrim(candidats.departement_source), '') is not null then 'staging'
        else 'non_renseigne'
    end as origine_departement
from candidats
left join communes
    on candidats.code_commune_insee = communes.code_commune_insee
