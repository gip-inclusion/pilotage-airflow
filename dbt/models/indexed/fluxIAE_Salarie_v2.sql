{{ config(
    materialized = 'table',
    indexes=[
      {'columns': ['salarie_id'], 'type' : 'btree', 'unique' : False},
    ]
 ) }}

select distinct
    {{ pilo_star(source('fluxIAE', 'fluxIAE_Salarie'), except=["hash_numéro_pass_iae"]) }},
    case
        when date_part('year', current_date) - date_part('year', to_date(salarie_annee_naissance::TEXT, 'YYYY')) <= 26 then 'Jeune (- de 26 ans)'
        when
            date_part('year', current_date) - date_part('year', to_date(salarie_annee_naissance::TEXT, 'YYYY')) > 25
            and date_part('year', current_date) - date_part('year', to_date(salarie_annee_naissance::TEXT, 'YYYY')) <= 49 then 'Adulte (26-49 ans)'
        when date_part('year', current_date) - date_part('year', to_date(salarie_annee_naissance::TEXT, 'YYYY')) >= 50 then 'Senior (50 ans et +)'
        else 'Non renseigné'
    end as tranche_age
from
    {{ source('fluxIAE', 'fluxIAE_Salarie') }}
