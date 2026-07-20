select
    imer.id,
    imer.date,
    imer.user_session,
    imer.user_kind,
    imer.user_id,
    imer.user_prescriber_organization_id,
    imer.user_company_id,
    imer.structure_id as source_structure_id,
    di_structure.structure_id,
    imer.kind,
    imer.service_id,
    imer.source,
    imer.external_link,
    imer.orientation_id,
    imer.date_mise_a_jour_metabase
from {{ ref('stg_emplois__imer') }} as imer
left join {{ ref('int_di__structure_source_mapping') }} as di_structure
    on imer.structure_id = di_structure.source_structure_id
