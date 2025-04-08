select
    {{ pilo_star(ref('suivi_etp_conventionnes_v2'), except=[
        "mpu_sct_mt_recet_nettoyage",
        "mpu_sct_mt_recet_serv_pers",
        "mpu_sct_mt_recet_btp",
        "mpu_sct_mt_recet_recycl",
        "mpu_sct_mt_recet_transp",
        "mpu_sct_mt_recet_autres"]) }},
    series_mois.date_annexe
from {{ ref('suivi_etp_conventionnes_v2') }} as suivi_etp
cross join {{ ref('eph_series_mois') }} as series_mois
where
    series_mois.date_annexe >= (suivi_etp.annee_af || '-01-01')::date
    and series_mois.date_annexe <= (suivi_etp.annee_af || '-12-31')::date
