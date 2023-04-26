with nb_siae_partenaires as (
    select
        organisations.id,
        count(distinct cel.id_structure) as nb_siae
    from
        {{ source('emplois', 'organisations') }} as organisations
    left join {{ ref('candidatures_echelle_locale') }} as cel
        on organisations.id = cel.id_org_prescripteur
    group by organisations.id
)
select
    organisations.*,
    -- appartenance geographique de l'organisation
    appartenance_geo_communes.libelle_commune,
    appartenance_geo_communes.code_insee,
    appartenance_geo_communes.nom_zone_emploi,
    -- nombre de siae partenaires de l'organisation =
    -- nombre de siae qui ont reçu une candidature de ce prescripteur
    nb_siae_partenaires.nb_siae
from {{ source('emplois', 'organisations') }} as organisations
left join {{ ref('stg_insee_appartenance_geo_communes') }} as appartenance_geo_communes
    on ltrim(organisations.code_commune, '0') = appartenance_geo_communes.code_insee
left join nb_siae_partenaires on organisations.id = nb_siae_partenaires.id
where organisations.nom != 'Regroupement des prescripteurs sans organisation'
