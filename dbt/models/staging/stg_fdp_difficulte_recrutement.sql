select
    {{ pilo_star(ref('stg_fdp_actives_sans_rec_30jrs_sans_motif_pasdeposteouvert')) }}
from stg_fdp_actives_sans_rec_30jrs_sans_motif_pasdeposteouvert
where delai_mise_en_ligne >= 30
