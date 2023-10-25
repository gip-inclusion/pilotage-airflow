select distinct
    salarie.salarie_id,
    salarie.salarie_rci_libelle,
    salarie.salarie_codeinseecom,
    salarie.salarie_commune              as commune,
    ltrim(salarie.salarie_code_dpt, '0') as departement,
    case
        when salarie.salarie_rci_libelle = 'MME' then 'Femme'
        when salarie.salarie_rci_libelle = 'M.' then 'Homme'
        else 'Non renseigné'
    end                                  as genre_salarie,
    case
        when (date_part('year', current_date) - salarie.salarie_annee_naissance) <= 25 then 'a- Moins de 26 ans'
        when (date_part('year', current_date) - salarie.salarie_annee_naissance) >= 26 and (date_part('year', current_date) - salarie.salarie_annee_naissance) <= 30 then 'b- Entre 26 ans et 30 ans'
        when (date_part('year', current_date) - salarie.salarie_annee_naissance) >= 31 and (date_part('year', current_date) - salarie.salarie_annee_naissance) <= 50 then 'c- Entre 31 ans et 50 ans'
        when (date_part('year', current_date) - salarie.salarie_annee_naissance) >= 51 then 'd- 51 ans et plus'
        else 'autre'
    end                                  as tranche_age,
    case
        when salarie.salarie_adr_qpv_type = 'QP' then 'Oui'
        else 'Non'
    end                                  as qpv,
    case
        when salarie.salarie_adr_is_zrr = 'true' then 'Oui'
        else 'Non'
    end                                  as zrr
from
    {{ ref('fluxIAE_Salarie_v2') }} as salarie
