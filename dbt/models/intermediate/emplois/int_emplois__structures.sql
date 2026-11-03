with emplois as (
    select * from {{ ref('stg_emplois__structures') }}
),

asp as (
    select * from {{ ref('stg_asp__structures') }}
),


final as (
    select
        emp.uid,
        emp.id,
        emp.id_asp                as id_strct_mere,
        emp.nom                   as nom_emplois,
        emp.type,
        emp.date_inscription      as date_inscription_emplois,
        emp.active,
        emp.convention_asp_active as conventionnee,
        emp.source,
        case
            when emp.source = 'Export ASP' then asp.structure_denomination
        end                       as nom_asp,
        case
            when emp.source = 'Export ASP' then coalesce(asp.structure_siret_actualise::text, emp.siret)
            else emp.siret
        end                       as siret,
        case
            when emp.source = 'Export ASP' then coalesce(asp.structure_code_naf, emp.code_naf)
            else emp.code_naf
        end                       as code_naf,
        case
            when emp.source = 'Export ASP' then coalesce(asp.structure_adresse_admin_code_insee, emp.code_commune_c1)
            else emp.code_commune_c1
        end                       as code_commune
    from emplois as emp
    left join asp as asp
        on emp.id_asp = asp.structure_id_siae
)

select * from final
