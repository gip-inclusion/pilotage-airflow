select
    acc_id,
    type_acc_pro,
    count(*) as nb
from {{ ref("stg_accompagnement_pro") }}
group by acc_id, type_acc_pro
having count(*) > 1
