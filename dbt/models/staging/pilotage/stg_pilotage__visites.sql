select
    id,
    user_id                            as id_utilisateur,
    user_kind                          as type_utilisateur,
    cast(dashboard_id as integer)      as id_tb,
    measured_at                        as date_visite,
    department                         as departement,
    region,
    current_company_id                 as id_structure,
    current_prescriber_organization_id as id_organisation,
    current_institution_id             as id_institution,
    "date_mise_à_jour_metabase"        as date_mise_a_jour_metabase
from {{ source('raw_emplois', 'c1_private_dashboard_visits_v0') }}
