select distinct
    {{ pilo_star(source('fluxIAE','fluxIAE_RefMontantIae'), relation_alias="ref_rsa") }},
    case
        when ref_rsa.code_dpt = 976 then '976 - Mayotte'
        else dept.departement_pilotage
    end as departement_pilotage,
    generate_series(
        date_trunc('month', to_date(rmi_date_debut_effet, 'DD/MM/YYYY')),
        case
            when rmi_date_fin_effet is null
                then date_trunc('month', date_trunc('year', current_date) + interval '1 year' - interval '1 day')
            else date_trunc('month', to_date(rmi_date_fin_effet, 'DD/MM/YYYY'))
        end,
        interval '1 month'
    )   as date_versement_rsa
from {{ source('fluxIAE', 'fluxIAE_RefMontantIae') }} as ref_rsa
cross join {{ ref('department_asp') }} as dept
where
    rmi_code = 'MT_ME_RSA'
    and (
        (ref_rsa.code_dpt is null and dept.departement_pilotage != '976 - Mayotte')
        or ref_rsa.code_dpt = 976
    )
