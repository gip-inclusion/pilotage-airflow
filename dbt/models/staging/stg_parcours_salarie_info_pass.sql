select
    salarie.hash_nir,
    (array_agg(salarie.salarie_commune order by salarie.salarie_date_modification desc))[1] as commune_candidat,
    (array_agg(
        left(lpad(salarie.salarie_codepostalcedex::text, 5, '0'), 2)
        order by salarie.salarie_date_modification desc
    ))[1]                                                                                   as code_departement_candidat,
    count(distinct pass_ag."hash_numéro_pass_iae")                                          as nombre_pass,
    min(pass_ag."date_début")                                                               as "date_début_premier_pass",
    max(pass_ag.date_fin)                                                                   as date_fin_dernier_pass,
    -- sortie_du_parcours peut être supprimée et remplacée par validite_dernier_pass une fois
    -- fais les modifs sur les TDB parcours
    case
        when max(pass_ag.date_fin) > current_date then 'Non'
        else 'Oui'
    end                                                                                     as sortie_du_parcours,
    (array_agg(pass_ag.validite_pass order by pass_ag.date_fin desc))[1]                    as validite_dernier_pass,
    (array_agg(pass_ag.suspension_en_cours order by pass_ag.date_fin desc))[1]              as suspension_en_cours_dernier_pass,
    (array_agg(pass_ag.motif_suspension order by pass_ag.date_fin desc))[1]                 as motif_suspension_dernier_pass,
    -- Ci-dessous je divise par 30.44 (durée moyenne d'un mois en jours) pour avoir des mois
    sum(extract(day from pass_ag."durée")::float) / 30.44                                   as "somme_durées_tous_pass_mois",
    array_agg(
        pass_ag."durée"
        order by pass_ag."date_début" asc
    )                                                                                       as "liste_durées_pass",
    extract(year from min(pass_ag."date_début"))                                            as "annee_début_premier_pass"
from {{ ref("pass_agrements_valides") }} as pass_ag
left join {{ source("fluxIAE","fluxIAE_Salarie") }} as salarie
    on pass_ag."hash_numéro_pass_iae" = salarie."hash_numéro_pass_iae"
where salarie.hash_nir is not null
group by salarie.hash_nir
