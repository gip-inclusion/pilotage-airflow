select
    {{ pilo_star(ref('stg_france_travail'),
    except=["C_CODEPOSTAL","C_TYPEPARCOURS", "C_SEXE", "C_SITUATIONFAMILLE", "ENFANTSACHARGE", "Freins"], relation_alias="ft") }},
    prcr.type_parcours      as "C_TYPEPARCOURS",
    sexe.sexe               as "C_SEXE",
    fam.situation_familiale as "C_SITUATIONFAMILLE",
    stock.stock             as stock_sans_en_cours_en_acces,
    /* when a bRSA does an immersion or a formation, it can be done outside its original
    department. This coalesce is aimed to get the original department of the job seeker and not the one
    of the immersion/formation */
    coalesce(
        dpt.territoire, first_value(
            dpt.territoire)
            over
            (partition by ft."N_INDIVIDU" order by case when dpt.territoire is not null then 0 else 1 end)
    )                       as "DEPARTEMENT",
    case
        when ft."ENFANTSACHARGE" = 0 then '0 enfant'
        when ft."ENFANTSACHARGE" = 1 then '1 enfant'
        when ft."ENFANTSACHARGE" = 2 then '2 enfants'
        when ft."ENFANTSACHARGE" >= 3 then '3 enfants et +'
        else 'Non renseigné'
    end                     as "ENFANTS",
    case
        when ft."AGE" between 0 and 30 then '1. - de 30'
        when ft."AGE" between 31 and 40 then '2. 30-40'
        when ft."AGE" between 41 and 50 then '4. 40-50'
        when ft."AGE" > 50 then '4. 50+'
    end                     as "tranche_d_age",
    case
        when ft."Freins" = 0 then 'b. 0 frein'
        when ft."Freins" = 1 then 'c. 1 frein'
        when ft."Freins" >= 2 then 'd. 2 freins et +'
        else 'a. Pas de diagnostic'
    end                     as "Freins",
    case
        when ft."C_TOPBOE" = 'O' then 'Oui'
        else 'Non'
    end                     as beneficiaire_de_l_obligation_a_l_emploi
from {{ ref('stg_france_travail') }} as ft
left join {{ ref('departements_xp_ft') }} as dpt
    on ft."C_CODEPOSTAL"::INTEGER = dpt.departement
left join {{ ref('type_parcours_xp_ft') }} as prcr
    on ft."C_TYPEPARCOURS" = prcr.parcours_raw
left join {{ ref('sexe_xp_ft') }} as sexe
    on ft."C_SEXE" = sexe.sexe_raw
left join {{ ref('situation_familiale_xp_ft') }} as fam
    on ft."C_SITUATIONFAMILLE" = fam.situation_familiale_raw
left join {{ ref('stock_xp_ft') }} as stock
    on ft."INDICATEUR ENTREE/SORTIE/STOCK" = stock.stock_raw
