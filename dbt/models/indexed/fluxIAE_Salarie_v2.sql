select distinct
    {{ pilo_star(source('fluxIAE', 'fluxIAE_Salarie'), except=["hash_numéro_pass_iae", "nir_chiffré"]) }},
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
-- an employee of an SIAE has a NIR & and NTT, we remove the NTT.
where "nir_chiffré" != 'gAAAAABqgwj3jmCreQOIfaUnHw_Jp_4i733EkKpj2grN7XfE6fU9ycH9WxiiP0-YKj8J3_ofTbl1uO_CMmSKePwjwCBPONZSoQ=='
