with candidatures_ph as (
    select
        /* FIXME(vperron): Normally, sqlfluff rendering star() should render a `*`.
        But for some reason it does not, yet, or not anymore.
        https://github.com/dbt-labs/dbt-utils/pull/732
        In the meantime, and since we don't want to fully maintain an intialized DB in CI,
        we have to workaround it. I would have like to monkeypatch it in CI (some override of
        the macro somewhere) but could not find a way to do it. Essentialy, we would want the
        following code being applied automatically. */
        {% if env_var('CI', ',') %}
            *,
        {% else %}
            {{ dbt_utils.star(ref('candidatures_echelle_locale')) }},
        {% endif %}
        replace("origine_détaillée", 'Prescripteur habilité ', '') as origine_simplifiee
    from {{ ref('candidatures_echelle_locale') }}
    where starts_with("origine_détaillée", 'Prescripteur habilité')
)
select
    candidatures_ph.*,
    org_libelles.libelle as libelle_complet
from candidatures_ph
left join {{ ref('organisation_libelles') }} as org_libelles
    on org_libelles.type = candidatures_ph.origine_simplifiee
