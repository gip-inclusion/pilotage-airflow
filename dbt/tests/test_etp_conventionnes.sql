with etp_sum as (
    select
        (
            --there some unique ids that are duplicated, we need to get rid of them
            select sum(af_etp_postes_insertion) from (
                select distinct on (af.af_id_annexe_financiere) af.af_etp_postes_insertion
                from {{ ref('stg_dates_annexe_financiere') }} as constantes
                cross join {{ ref('fluxIAE_AnnexeFinanciere_v2') }} as af
                where
                    date_part('year', af.af_date_debut_effet_v2) = constantes.annee_en_cours
                    and af.af_etat_annexe_financiere_code in (
                        'VALIDE',
                        'PROVISOIRE',
                        'CLOTURE'
                    )
                    and af.af_mesure_dispositif_code not like '%FDI%'
            ) as tmp
        ) as theirs,
        (
            select sum(etp."effectif_mensuel_conventionné")
            from {{ ref('stg_dates_annexe_financiere') }} as constantes
            cross join {{ ref("suivi_etp_conventionnes_v2") }} as etp
            where
                etp.annee_af = constantes.annee_en_cours
        ) as ours
)

select *
from etp_sum
where round(theirs) != round(ours)
