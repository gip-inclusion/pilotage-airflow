create table igas_suivi_salarie_par_annee as
select
    id_salarie,
    annee_af,
    sum(nombre_heures_travaillees) as nombre_heures_travaillees,
    sum(nombre_etp_consommes_reels_annuels) as nombre_etp_consommes_reels_annuels,
    civilite,
    annee_de_naissance,
    sum(nb_formations) nb_formations,
    sum(nb_jours_formation) nb_jours_formation,
    sum(nb_heures_formation) nb_heures_formation,
    rqth,
    aah,
    oeth,
    ass,
    zrr,
    aide_soc,
    qpv
from
    igas_eiti_salarie
group by
    id_salarie,
    annee_af,
    civilite,
    annee_de_naissance,
    rqth,
    aah,
    oeth,
    ass,
    zrr,
    aide_soc,
    qpv;
