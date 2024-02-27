select
    {{ pilo_star(source('oneshot','FT_donnees_brutes'),
        except=['C_CODEPOSTAL',"D_DATENAISSANCE", "T_DATEEXTRACTION",'D_ENTREEPARCOURS',"C_INDICATEUR"]) }},
    "C_INDICATEUR"                            as "INDICATEUR ENTREE/SORTIE/STOCK",
    to_date("D_DATENAISSANCE", 'YYYY-MM')     as "D_DATENAISSANCE",
    to_date("D_ENTREEPARCOURS", 'YYYY-MM')    as "D_ENTREEPARCOURS",
    to_date("T_DATEEXTRACTION", 'YYYY-MM-DD') as "T_DATEEXTRACTION",
    to_date("D_JOUREVENEMENT", 'YYYY-MM-DD')  as "SEMAINE",
    date_part('year', age(
        to_date("T_DATEEXTRACTION", 'YYYY-MM-DD'),
        to_date("D_DATENAISSANCE", 'YYYY-MM')
    ))                                        as "AGE",
    case
        when "C_NOMBRESENFANTACHARGE" = 'YYYY' then null
        else "C_NOMBRESENFANTACHARGE"::INTEGER
    end                                       as "ENFANTSACHARGE",
    case
        when "C_CODEPOSTAL" = 'YYYY' then null
        else substring("C_CODEPOSTAL", 1, 2)
    end
    as "C_CODEPOSTAL",
    --ask Claire what we should do with "FREIN_ENTREE_FORMATION"
    (
        "FREIN_ENTREE_FAMILLE" + "FREIN_ENTREE_ADMIN" + "FREIN_ENTREE_FINANCE" + "FREIN_ENTREE_LOGEMENT" + "FREIN_ENTREE_LANGUESAVOIR"
        + "FREIN_ENTREE_MOBILITE" + "FREIN_ENTREE_NUMERIQUE" + "FREIN_ENTREE_SANTE"
    )                                         as "Freins"
from
    {{ source('oneshot','FT_donnees_brutes') }}
