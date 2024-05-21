select
    svtp."Tableau de bord"   as tableau_de_bord,
    'TB privé'               as type_de_tb,
    svtp."Département",
    svtp."Nom Département"   as "nom_département",
    svtp."Date"              as semaine,
    svtp."Visiteurs uniques" as visiteurs_uniques
from
    /* Ancienne table créée par Victor qui débute en 2022 et s'arrête à la semaine du 12/12/22 */
    {{ source('oneshot', 'suivi_visiteurs_tb_prives') }} as svtp
