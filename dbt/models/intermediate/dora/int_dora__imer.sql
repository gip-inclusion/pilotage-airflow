select
    'orientation'                                                                       as kind,
    cast(orientation_id as text)                                                        as event_id,
    orientation_creation_date                                                           as date, -- noqa: RF04
    orientation_prescriber_id                                                           as user_id,
    user_main_activity                                                                  as user_kind,
    user_is_manager                                                                     as is_manager,
    structure_typology,
    structure_department,
    coalesce(orientation_di_service_id is not null, false)                              as is_di_service,
    coalesce(user_main_activity in ('accompagnateur', 'accompagnateur_offreur'), false) as is_prescriber,
    null                                                                                as generates_orientation
from {{ ref('int_dora__orientation_user_service') }}
union all
select
    'mobilisation'                                                                        as kind,
    m.event_id,
    m.event_date                                                                          as date, -- noqa: RF04
    m.user_id,
    m.event_user_kind                                                                     as user_kind,
    m.event_is_manager                                                                    as is_manager,
    struct_members.structure_typology,
    struct_members.structure_department,
    m.event_is_di                                                                         as is_di_service,
    coalesce(m.user_main_activity in ('accompagnateur', 'accompagnateur_offreur'), false) as is_prescriber,
    o_m.generates_orientation
from {{ ref('int_dora__mobilisationevent_user') }} as m
left join
    {{ ref('int_dora__structure_members') }} as struct_members
    on m.user_id = struct_members.user_id and m.event_structure_id = cast(struct_members.structure_id as text)
left join {{ ref('int_dora__mobilisation_to_orientation') }} as o_m
    on m.event_id = o_m.mobilisation_id
