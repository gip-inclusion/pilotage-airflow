{% macro count_intensite_frein(column, alias) %}
    count(*) filter (where {{ column }} = 'NON_RENSEIGNE') as {{ alias }}_non_renseigne,
    count(*) filter (where {{ column }} = 'FAIBLE')        as {{ alias }}_faible,
    count(*) filter (where {{ column }} = 'MOYEN')         as {{ alias }}_moyen,
    count(*) filter (where {{ column }} = 'FORT')          as {{ alias }}_fort
{%- endmacro %}
