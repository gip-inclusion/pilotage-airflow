select
    {{ pilo_star(ref('stg_fdp_actives_sans_rec_30jrs')) }},
    '3- Fiches de poste actives sans recrutement dans les 30 derniers jours' as etape,
    nb_fdp_struct                                                            as valeur
from stg_fdp_actives_sans_rec_30jrs
union all
select
    {{ pilo_star(ref('stg_fdp_actives_sans_rec_30jrs_sans_motif_pasdeposteouvert')) }},
    '4- Fiches de poste actives sans recrutement dans les 30 derniers jours et sans motif pas de poste ouvert' as etape,
    nb_fdp_struct                                                                                              as valeur
from stg_fdp_actives_sans_rec_30jrs_sans_motif_pasdeposteouvert
