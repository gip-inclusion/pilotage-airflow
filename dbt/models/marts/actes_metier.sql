with unioned as (

    select
        mois::text as mois,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        departement,
        nombre_actes
    from {{ ref('int_rdvi__actes_metiers') }}

    union all

    select
        mois::text as mois,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        departement,
        nombre_actes
    from {{ ref('int_emplois__actes_metier') }}

    union all

    select
        to_char(mois, 'YYYY-MM') as mois,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        departement,
        nombre_actes
    from {{ ref('int_matomo__actes_metier') }}

    union all

    select
        mois::text as mois,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        departement,
        nombre_actes
    from {{ ref('int_gps__actes_metier') }}

    union all

    select
        mois::text as mois,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        departement,
        nombre_actes
    from {{ ref('int_dora__actes_metier') }}

    union all

    select
        mois::text as mois,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        departement,
        nombre_actes
    from {{ ref('int_monrecap__actes_metier') }}

    union all

    select
        mois::text as mois,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        departement,
        nombre_actes
    from {{ ref('int_marche__actes_metier') }}

    union all

    select
        mois::text as mois,
        source,
        type_acte,
        categorie_acte,
        north_star,
        north_star_70,
        traite,
        type_structure,
        departement,
        nombre_actes
    from {{ ref('int_data_inclusion__actes_metier') }}

)

select
    mois,
    source,
    type_acte,
    categorie_acte,
    north_star,
    north_star_70,
    traite,
    type_structure,
    departement,
    sum(nombre_actes)::integer as nombre_actes
from unioned
where nombre_actes > 0
group by
    mois,
    source,
    type_acte,
    categorie_acte,
    north_star,
    north_star_70,
    traite,
    type_structure,
    departement
