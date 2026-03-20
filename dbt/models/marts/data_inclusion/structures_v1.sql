select * from {{ source('data_inclusion', 'structures_v1_with_soliguide') }}
where source != 'soliguide'
