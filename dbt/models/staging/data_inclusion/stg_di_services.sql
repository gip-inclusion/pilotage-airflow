select {{ dbt_utils.star(source('data_inclusion', 'services_v1_with_soliguide')) }} from {{ source('data_inclusion', 'services_v1_with_soliguide') }}
