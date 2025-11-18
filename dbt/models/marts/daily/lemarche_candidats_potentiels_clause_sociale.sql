select
    coalesce(c_p_sc.hash_nir, c_ra.hash_nir)                                   as hash_nir,
    coalesce(c_p_sc.code_departement_candidat, c_ra.code_departement_candidat) as code_departement_candidat,
    coalesce(c_p_sc.commune_candidat, c_ra.commune_candidat)                   as commune_candidat,
    case when c_p_sc.hash_nir is not null then 'Oui' else 'Non' end            as candidat_sans_contrat_pass_candidat,
    case when c_ra.hash_nir is not null then 'Oui' else 'Non' end              as candidat_recherche_active,
    case
        when c_p_sc.hash_nir is not null and c_ra.hash_nir is not null then 'file_active_et_pass_valide'
        when c_ra.hash_nir is not null then 'file_active_critere_niveau_1'
        when c_p_sc.hash_nir is not null then 'pass_valide_sans_contrat'
    end                                                                        as statut
from {{ ref('eph_candidats_sans_contrat_pass_valide') }} as c_p_sc
full outer join {{ ref('stg_candidats_file_active_critere_1') }} as c_ra
    on c_p_sc.hash_nir = c_ra.hash_nir
