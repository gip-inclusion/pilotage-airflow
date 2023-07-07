select max(date_part('year', af_date_debut_effet_v2)) as annee_en_cours
from
    {{ ref('fluxIAE_AnnexeFinanciere_v2') }}
