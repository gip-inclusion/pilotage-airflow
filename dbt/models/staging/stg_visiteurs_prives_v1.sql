select
    svtp0."Tableau de bord"                    as tableau_de_bord,
    'TB privé'                                 as type_de_tb,
    svtp0."Département",
    svtp0."Nom Département"                    as "nom_département",
    to_date(svtp0."Date", 'YYYY-MM-DD')        as semaine,
    to_number(svtp0."Unique visitors", '9999') as visiteurs_uniques
from
    /* Nouvelle table créée par Victor qui démarre le 01/01/22 */
    {{ source('oneshot', 'suivi_visiteurs_tb_prives_v1') }} as svtp0
where to_date(svtp0."Date", 'YYYY-MM-DD') < '2023-03-13'
