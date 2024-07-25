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
-- On regarde les candidatures refusées par l'origine_id_structure
left join {{ ref('stg_candidats_candidatures_refusees') }} as cca
    on cddr.id_candidat = cca.id_candidat and cddr.origine_id_structure = cca.id_structure
-- On veut uniquement voir les candidatures qui ont pour origine un employeur orienteur et qui ont été acceptées suite à cette orientation
where cddr.origine = 'Employeur orienteur' and cddr."état" = 'Candidature acceptée' and cca.id_candidat is not null
