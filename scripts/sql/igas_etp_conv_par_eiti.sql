select distinct
    structure.structure_id_siae as id_struct,
    structure.structure_denomination as denomination_structure,
    af.af_numero_annexe_financiere as id_annexe_financiere,
    date_part('year',
        af.af_date_debut_effet_v2) as annee_af,
    sum((af.af_etp_postes_insertion * ((date_part('year',
                af_date_fin_effet_v2) - date_part('year',
                af_date_debut_effet_v2)) * 12 + (date_part('month',
                af_date_fin_effet_v2) - date_part('month',
                af_date_debut_effet_v2)) + 1) / 12)) as effectif_annuel_conv
from
    "fluxIAE_AnnexeFinanciere_v2" as af
left join "fluxIAE_Structure_v2" as structure on af.af_id_structure = structure.structure_id_siae
where
    af.af_mesure_dispositif_code = 'EITI_DC'
    and af.af_etat_annexe_financiere_code in('VALIDE',
        'PROVISOIRE',
        'CLOTURE')
    and af_mesure_dispositif_code not like '%FDI%'
group by
    id_struct,
    structure.structure_denomination,
    af.af_numero_annexe_financiere,
    annee_af;
