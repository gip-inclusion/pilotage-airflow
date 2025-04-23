with etp_sum as (
    select
        (
            select sum(emi_part_etp) from (
                select distinct on (emi.emi_dsm_id) emi.emi_part_etp
                from {{ ref('eph_dates_etat_mensuel_individualise') }} as constantes
                cross join {{ source('fluxIAE', 'fluxIAE_EtatMensuelIndiv') }} as emi
                left join {{ ref('fluxIAE_AnnexeFinanciere_v2') }} as af
                    on emi.emi_afi_id = af.af_id_annexe_financiere
                left join {{ source('fluxIAE', 'fluxIAE_RefMontantIae') }} as firmi
                    on af.af_mesure_dispositif_id = firmi.rme_id
                where
                    emi.emi_sme_annee = constantes.annee_en_cours
                    and firmi.rmi_libelle = 'Nombre d''heures annuelles théoriques pour un salarié à taux plein'
                    and af.af_etat_annexe_financiere_code in (
                        'VALIDE',
                        'PROVISOIRE',
                        'CLOTURE'
                    )
                    and af.af_mesure_dispositif_code not like '%FDI%'
            ) as tmp
        ) as theirs,
        (
            select sum(etp.nombre_etp_consommes_asp)
            from {{ ref('eph_dates_etat_mensuel_individualise') }} as constantes
            cross join {{ ref("suivi_etp_realises_v2") }} as etp
            where
                etp.emi_sme_annee = constantes.annee_en_cours
        ) as ours
)

select *
from etp_sum
where round(theirs) != round(ours)
