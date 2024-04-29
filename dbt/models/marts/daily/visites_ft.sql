select
    vp."Tableau de bord"                    as tableau_de_bord,
    to_date(vp."Date", 'YYYY-MM-DD')        as semaine,
    to_number(vp."Visits", '9999')          as visites,
    to_number(vp."Unique visitors", '9999') as visiteurs_uniques
from
    /* Nouvelle table créée par Victor qui reprend toutes les infos des visiteurs des TBs publics */
    {{ source('matomo', 'suivi_visiteurs_tb_publics_v1') }} as vp
where vp."Tableau de bord" = 'tb 406 - Requêtage des données de l''expérimentation RSA'
