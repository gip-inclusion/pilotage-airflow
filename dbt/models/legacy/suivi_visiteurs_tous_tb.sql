/* Dans cette table nous récupérons les infos des visiteurs uniques de Matomo à partir des tables créées par Victor qui automatisent les requêtes API */

with visiteurs_prives as (
    select
        svtp."Date"              as semaine,
        /* Obligé de mettre les titres des colonnes entre "" sinon j'avais un message d'erreur de mon GUI */
        svtp."Tableau de bord"   as tableau_de_bord,
        svtp."Visiteurs uniques" as visiteurs_uniques,
        'TB privé'               as type_de_tb
    from
        {{ source('oneshot', 'suivi_visiteurs_tb_prives') }} as svtp /* Ancienne table créée par Victor qui débute en 2022 et s'arrête à la semaine du 12/12/22 */
),

visiteurs_prives_0 as (
    select
        svtp0."Tableau de bord"                    as tableau_de_bord,
        'TB privé'                                 as type_de_tb,
        to_date(svtp0."Date", 'YYYY-MM-DD')        as semaine,
        to_number(svtp0."Unique visitors", '9999') as visiteurs_uniques
    from
        {{ source('matomo', 'suivi_visiteurs_tb_prives_v1') }} as svtp0 /* Nouvelle table créée par Victor qui démarre le 01/01/22 */
),

visiteurs_publics as (
    select
        vp."Tableau de bord"                    as tableau_de_bord,
        'TB public'                             as type_de_tb,
        to_date(vp."Date", 'YYYY-MM-DD')        as semaine,
        to_number(vp."Unique visitors", '9999') as visiteurs_uniques
    from
        {{ source('matomo', 'suivi_visiteurs_tb_publics_v1') }} as vp /* Nouvelle table créée par Victor qui reprend toutes les infos des visiteurs des TBs publics */
)

select
    semaine,
    tableau_de_bord,
    visiteurs_uniques,
    type_de_tb
from
    visiteurs_prives
where
    semaine is not null
union all
select
    semaine,
    tableau_de_bord,
    visiteurs_uniques,
    type_de_tb
from
    visiteurs_prives_0
where
    semaine is not null
union all
select
    semaine,
    tableau_de_bord,
    visiteurs_uniques,
    type_de_tb
from
    visiteurs_publics