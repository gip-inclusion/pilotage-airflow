select {{ dbt_utils.star(source('data_inclusion', 'services_v1')) }} from {{ source('data_inclusion', 'services_v1') }}
