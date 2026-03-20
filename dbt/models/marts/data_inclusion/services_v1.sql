select * from {{ source('data_inclusion', 'services_v1_with_soliguide') }}
where source != 'soliguide'
