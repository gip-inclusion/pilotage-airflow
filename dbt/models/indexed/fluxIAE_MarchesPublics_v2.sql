{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['mpu_af_id'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select distinct
    mpu_af_id,
    mpu_sct_mt_recet_nettoyage,
    mpu_sct_mt_recet_serv_pers,
    mpu_sct_mt_recet_btp,
    mpu_sct_mt_recet_agri,
    mpu_sct_mt_recet_recycl,
    mpu_sct_mt_recet_transp,
    mpu_sct_mt_recet_autres
from
    {{ source('fluxIAE', 'fluxIAE_MarchesPublics') }}
where not (
    coalesce(mpu_sct_mt_recet_nettoyage, 0) = 0
    and coalesce(mpu_sct_mt_recet_serv_pers, 0) = 0
    and coalesce(mpu_sct_mt_recet_btp, 0) = 0
    and coalesce(mpu_sct_mt_recet_agri, 0) = 0
    and coalesce(mpu_sct_mt_recet_recycl, 0) = 0
    and coalesce(mpu_sct_mt_recet_transp, 0) = 0
    and coalesce(mpu_sct_mt_recet_autres, 0) = 0
)
