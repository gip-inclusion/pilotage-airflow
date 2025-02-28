-- WIP!!
select
    hash_nir,
 --   af.nom_region_af
    --split_part(string_agg(af.nom_region_af, ',' order by ctr.contrat_date_embauche desc), ',', 1)     as region,
  --  split_part(string_agg(af.nom_departement_af ',' order by ctr.contrat_date_embauche desc), ',', 1) as departement,
    count(distinct ctr.contrat_id_structure)                                                              as nombre_structures,
    count(distinct ctr.contrat_mesure_disp_code)                                                          as nombre_type_dispositifs_differents,
    count(distinct ctr.contrat_parent_id)                                                                 as nombre_contrats_differents,
    string_agg(ctr.contrat_mesure_disp_code, ',' order by ctr.contrat_date_embauche)                  as list_tous_type_dispositifs,
    -- ci-dessous si on fait distinct, on perd l'info chronologique car on peut avoir type1, type2, type2
    string_agg(distinct ctr.contrat_mesure_disp_code, ',')                                            as list_diff_type_dispositifs,
    sum(
        case
            when ctr.contrat_date_sortie_definitive is not null
                then
                    ctr.contrat_date_sortie_definitive - ctr.contrat_date_embauche
            else current_date - ctr.contrat_date_embauche
        end
    )                                                                                                 as "durée_tous_contrats"
from {{ ref("stg_contrats_parent") }} as ctr
left join {{ ref("eph_stg_uniques_salarie_id") }} as salarie
    on ctr.contrat_id_pph = salarie.salarie_id
--left join {{ ref("stg_info_contrat_AnnexeFinanciere_v2") }} as af
--    on ctr.contrat_id_structure = af.af_id_structure and ctr.contrat_mesure_disp_code = af.af_mesure_dispositif_code
group by hash_nir
