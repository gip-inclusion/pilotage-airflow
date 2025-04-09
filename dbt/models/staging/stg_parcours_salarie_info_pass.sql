select
    salarie.hash_nir,
    count(distinct pass_ag."hash_numéro_pass_iae")        as nombre_pass,
    min(pass_ag."date_début")                             as "date_début_premier_pass",
    max(pass_ag.date_fin)                                 as date_fin_dernier_pass,
    case
        when max(pass_ag.date_fin) > current_date then 'Non'
        else 'Oui'
    end                                                   as sortie_du_parcours,
    sum(extract(day from pass_ag."durée")::float) / 30.44 as "somme_durées_tous_pass_mois",
    array_agg(
        pass_ag."durée"
        order by pass_ag."date_début" asc
    )                                                     as "liste_durées_pass",
    extract(year from min(pass_ag."date_début"))          as "annee_début_premier_pass"
from {{ source("emplois","pass_agréments") }} as pass_ag
left join {{ source("fluxIAE","fluxIAE_Salarie") }} as salarie
    on pass_ag."hash_numéro_pass_iae" = salarie."hash_numéro_pass_iae"
where pass_ag.type != 'Agrément PE' and pass_ag.type != 'Agrément PE via ITOU (non 99999)' and salarie.hash_nir is not null
group by salarie.hash_nir
