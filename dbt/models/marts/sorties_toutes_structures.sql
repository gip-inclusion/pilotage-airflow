select
    {{ pilo_star(ref('stg_sorties_aci_ei'),
    except=["duree_contrat_regles_asp", "nombre_mois_travailles", "total_heures_travaillees_derniere_af", "duree_contrat_regles_asp"]) }}
from {{ ref('stg_sorties_aci_ei') }}
union all
select
    {{ pilo_star(ref('stg_sorties_ai_etti'), except=["total_heures_travaillees"]) }}
from {{ ref('stg_sorties_ai_etti') }}
union all
select {{ pilo_star(ref('stg_sorties_eiti')) }}
from {{ ref('stg_sorties_eiti') }}
