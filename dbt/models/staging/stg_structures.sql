select
    s.id,
    s.id_asp,
    s.nom,
    s.type                                     as type_struct,
    s.siret,
    s.active,

    s.code_commune,
    dim_commune.code_departement_insee         as "département",

    dim_commune.nom_departement                as "nom_département",
    dim_commune.nom_departement_complet,
    dim_commune.nom_region                     as "région",
    dim_commune.nom_arrondissement,
    dim_commune.nom_epci                       as nom_epci_structure,
    dim_commune.nom_zone_emploi                as bassin_d_emploi,
    s.nom_complet                              as nom_structure_complet,

    coalesce(dim_commune.nom_commune, s.ville) as ville,

    case
        when s.convergence_france = 1 then 'Oui'
        else 'Non'
    end                                        as structure_convergence

from
    {{ ref('structures') }} as s

left join
    {{ ref('dim_commune') }}
    on s.code_commune = dim_commune.code_commune_insee
