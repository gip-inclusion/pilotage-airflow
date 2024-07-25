select
    cddr.id,
    cddr.id_candidat,
    cca.id_candidat as id_candidat_bis,
    cddr.id_structure,
    cddr.origine_id_structure,
    cddr.origine,
    cddr.type_structure,
    cddr.nom_structure,
    cddr."état"
from {{ ref('stg_candidatures') }} as cddr
left join {{ ref('stg_candidats_candidatures_employeurs_orienteurs') }} as cca
    on cddr.id_candidat = cca.id_candidat and cddr.origine_id_structure = cca.id_structure
-- On veut uniquement voir les candidatures qui ont pour origine un employeur orienteur
where cddr.origine = 'Employeur orienteur'
