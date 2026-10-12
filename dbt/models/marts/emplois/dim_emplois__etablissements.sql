with structures as (
    select
        *,
        count(*) over (partition by siret) as nb_siret
    from {{ ref('int_emplois__structures') }}
),

organisations as (
    select
        *,
        count(*) over (partition by siret)                               as nb_siret,
        count(*) filter (where type = 'Autre') over (partition by siret) as nb_siret_autre
    from {{ ref('stg_emplois__organisations') }}
),

-- Rapprochement 1:1 par SIRET : structure seule sur son SIRET et organisation seule
-- ou unique organisation de type Autre parmi celles partageant le SIRET
correspondances as (
    select
        s.uid as uid_structure,
        o.uid as uid_organisation
    from structures as s
    inner join organisations as o on s.siret = o.siret
    where
        s.nb_siret = 1
        and (o.nb_siret = 1 or (o.type = 'Autre' and o.nb_siret_autre = 1))
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['s.uid', 'o.uid']) }} as id,
        s.uid                                                      as uid_structure,
        s.id                                                       as id_structure,
        o.uid                                                      as uid_organisation,
        o.id                                                       as id_organisation,
        s.nom_emplois                                              as nom_structure,
        o.nom                                                      as nom_organisation,
        s.id_strct_mere,
        s.nom_asp                                                  as nom_structure_asp,
        s.type                                                     as type_structure,
        o.type                                                     as type_organisation,
        o.type_complet,
        o.habilitee,
        s.code_naf,
        o.code_safir,
        s.source,
        case
            when s.uid is null then 'organisation'
            when o.uid is null then 'structure'
            else 'structure_organisation'
        end                                                        as type_etablissement,
        coalesce(s.siret, o.siret)                                 as siret,
        coalesce(s.code_commune, o.code_commune)                   as code_commune,
        least(s.date_inscription_emplois, o.date_inscription)      as date_inscription_emplois,
        greatest(s.active, o.active)                               as active
    from structures as s
    left join correspondances as c on s.uid = c.uid_structure
    full outer join organisations as o on c.uid_organisation = o.uid
)

select * from final
