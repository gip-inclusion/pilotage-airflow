{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['mpu_af_id'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

with ranked_data as (
    select
        mpu_af_id,
        mpu_sct_mt_recet_nettoyage,
        mpu_sct_mt_recet_serv_pers,
        mpu_sct_mt_recet_btp,
        mpu_sct_mt_recet_agri,
        mpu_sct_mt_recet_recycl,
        mpu_sct_mt_recet_transp,
        mpu_sct_mt_recet_autres,
        row_number() over (
            partition by mpu_af_id
            order by to_timestamp(mpu_date_creation, 'DD/MM/YYYY HH24:MI:SS') desc
        ) as rn
    from {{ source('fluxIAE', 'fluxIAE_MarchesPublics') }}
    where {{ sum_nullable_columns([
        'mpu_sct_mt_recet_nettoyage',
        'mpu_sct_mt_recet_serv_pers',
        'mpu_sct_mt_recet_btp',
        'mpu_sct_mt_recet_agri',
        'mpu_sct_mt_recet_recycl',
        'mpu_sct_mt_recet_transp',
        'mpu_sct_mt_recet_autres'
    ]) }} != 0
)

select
    mpu_af_id,
    mpu_sct_mt_recet_nettoyage,
    mpu_sct_mt_recet_serv_pers,
    mpu_sct_mt_recet_btp,
    mpu_sct_mt_recet_agri,
    mpu_sct_mt_recet_recycl,
    mpu_sct_mt_recet_transp,
    mpu_sct_mt_recet_autres
from ranked_data
where rn = 1
