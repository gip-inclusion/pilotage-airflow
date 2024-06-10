select
    cm.contrat_id_structure,
    struct.structure_denomination,
    rfc.rfc_lib_forme_contrat                                          as type_contrat,
    extract(year from to_date(cm.contrat_date_embauche, 'DD/MM/YYYY')) as annee_embauche,
    count(cm.contrat_id_ctr)                                           as nb_contrats
from {{ source("fluxIAE", "fluxIAE_ContratMission") }} as cm
left join {{ source("fluxIAE", "fluxIAE_RefFormeContrat") }} as rfc
    on cm."contrat_format_contrat_code" = rfc."rfc_code_forme_contrat"
left join {{ source("fluxIAE", "fluxIAE_Structure") }} as struct
    on cm.contrat_id_structure = struct.structure_id_siae
where
    rfc_lib_forme_contrat = 'CDDI PHC'
    or rfc_lib_forme_contrat = 'CDDI CVG'
group by
    cm.contrat_id_structure,
    struct.structure_denomination,
    rfc.rfc_lib_forme_contrat,
    extract(year from to_date(cm.contrat_date_embauche, 'DD/MM/YYYY'))
