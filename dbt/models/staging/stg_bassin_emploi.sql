select
    be.libelle_commune      as ville,
    be.type_epci,
    be.nom_departement,
    be.nom_region,
    be.nom_epci,
    be.code_commune,
    be.nom_arrondissement,
    /* zone d'emploi = bassin d'emploi */
    be.nom_zone_emploi_2020 as bassin_d_emploi,
    /* on récupère que l'id des structures de la table structure */
    s.id                    as id_structure
from {{ source('oneshot', 'sa_zones_infradepartementales') }} as be
left join {{ source('emplois', 'structures') }} as s
    /* il faut rajouter le département car la France n'est pas originale en terme de noms de ville */
    on s.ville = be.libelle_commune and s."nom_département" = be.nom_departement
