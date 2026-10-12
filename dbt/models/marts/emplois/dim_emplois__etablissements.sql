with structures as (
    select * from {{ ref('int_emplois__structures') }}
),

organisations as (
    select * from {{ ref('stg_emplois__organisations') }}
),

final as (
    select
        'structure'   as type_etablissement,
        uid,
        id,
        id_strct_mere,
        nom_emplois,
        nom_asp,
        type,
        null          as type_complet,
        null::integer as habilitee,
        siret,
        code_naf,
        code_commune,
        null          as code_safir,
        source,
        date_inscription_emplois,
        active
    from structures

    union all

    select
        'organisation'   as type_etablissement,
        uid,
        id,
        null::integer    as id_strct_mere,
        nom              as nom_emplois,
        null             as nom_asp,
        type,
        type_complet,
        habilitee,
        siret,
        null             as code_naf,
        code_commune,
        code_safir,
        null             as source,
        date_inscription as date_inscription_emplois,
        active
    from organisations
)

select * from final
