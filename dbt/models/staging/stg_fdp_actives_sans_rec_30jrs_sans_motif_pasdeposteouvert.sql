select
    {{ pilo_star(ref('stg_fdp_actives_sans_rec_30jrs')) }}
from stg_fdp_actives_sans_rec_30jrs
where not refus_30_jours_pas_de_poste
