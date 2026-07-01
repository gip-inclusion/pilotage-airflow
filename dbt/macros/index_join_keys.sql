{% macro index_join_keys() %}
  {% if execute and model.config.materialized == 'table' %}
    {#- extra columns indexed manually because they don't match the id pattern -#}
    {%- set extra = ['hash_nir', 'finess_num', 'code_commune_insee',
                     'code_commune_parente', 'code_rome'] -%}
    {#- `this` is a dbt built-in: the model being built -#}
    {%- for col in adapter.get_columns_in_relation(this) -%}
      {%- set n = col.name | lower -%}
      {%- if 'id' in n.split('_') or n in extra %}
        {#- 63 = psql's identifier-length limit -#}
        {%- set idx = (this.identifier ~ '_' ~ n ~ '_idx') | truncate(63, true, '') %}
        create index if not exists "{{ idx }}" on {{ this }} ("{{ col.name }}");
      {%- endif -%}
    {%- endfor -%}
  {% endif %}
{% endmacro %}
