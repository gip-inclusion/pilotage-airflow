select
    {{ pilo_star(ref('stg_contrats')) }},
    structs.nom_region_structure,
    structs.nom_departement_structure,
    structs.code_dept_structure,
    first_value(ctr.contrat_id_ctr) over (
        partition by ctr.contrat_id_pph, groupe_contrat
        order by ctr.contrat_date_embauche
    ) as contrat_parent_id
from {{ ref('stg_contrats') }} as ctr
left join {{ ref('fluxIAE_Structure_v2') }} as structs
    on ctr.contrat_id_structure = structs.structure_id_siae
