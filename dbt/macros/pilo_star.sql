{% macro pilo_star(table_name, relation_alias=False, except=[], prefix='', suffix='', quote_identifiers=True) -%}
    /* FIXME(vperron): Normally, sqlfluff rendering star() should render a `*`.
    But for some reason it does not, yet, or not anymore.
    https://github.com/dbt-labs/dbt-utils/pull/732
    In the meantime, and since we don't want to fully maintain an intialized DB in CI,
    we have to workaround it. I would have like to monkeypatch it in CI (some override of
    the macro somewhere) but could not find a way to do it. Essentialy, we would want the
    following code being applied automatically. */
    {%- if env_var('CI', '') -%}
        id
    {%- else -%}
        {{ dbt_utils.star(table_name, relation_alias, except, prefix, suffix, quote_identifiers) }}
    {%- endif -%}
{% endmacro %}

