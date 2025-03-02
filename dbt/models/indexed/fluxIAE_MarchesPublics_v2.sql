{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['mpu_af_id'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select distinct --sometimes there are duplicates, we want to avoid this
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
where {{ sum_nullable_columns([
    'mpu_sct_mt_recet_nettoyage',
    'mpu_sct_mt_recet_serv_pers',
    'mpu_sct_mt_recet_btp',
    'mpu_sct_mt_recet_agri',
    'mpu_sct_mt_recet_recycl',
    'mpu_sct_mt_recet_transp',
    'mpu_sct_mt_recet_autres'
]) }} != 0
