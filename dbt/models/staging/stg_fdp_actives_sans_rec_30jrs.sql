-- fiches de poste sans candidatures sur les 30 derniers jours ou avec des candidatures mais sans recrutement
select
    {{ pilo_star(ref('stg_fdp_actives')) }}
from {{ ref('stg_fdp_actives') }}
where not candidature_30_derniers_jours or not embauche_30_derniers_jours
