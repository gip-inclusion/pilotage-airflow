select
    id_annexe_financiere,
    af_numero_annexe_financiere,
    -- On prend le mois maximal saisi +1 - le mois minimal saisi pour avoir le nombre de mois saisis
    (max(date_part('month', date_saisie)) + 1) - min(date_part('month', date_saisie)) as nombre_mois_saisis
from
    {{ ref('suivi_etp_realises_v2') }}
group by
    id_annexe_financiere,
    af_numero_annexe_financiere
