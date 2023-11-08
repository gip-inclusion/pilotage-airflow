--drop table igas_suivi_contrat_par_annee;
create table igas_suivi_contrat_par_annee as
select
    contrat_id,
    annee_af,
    sum(nombre_heures_travaillees) as nombre_heures_travaillees,
    date_embauche,
    sum(nombre_etp_consommes_reels_annuels) as nombre_etp_consommes_reels_annuels,
    motif_sortie,
    niveau_formation_salarie,
    civilite,
    age,
    annee_de_naissance,
    sum(nb_formations) nb_formations,
    sum(nb_jours_formation) nb_jours_formation,
    sum(nb_heures_formation) nb_heures_formation,
    brsa,
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
    contrat_id,
    motif_sortie,
    niveau_formation_salarie,
    date_embauche,
    annee_af,
    civilite,
    age,
    annee_de_naissance,
    brsa,
    rqth,
    aah,
    oeth,
    ass,
    zrr,
    aide_soc,
    qpv;
