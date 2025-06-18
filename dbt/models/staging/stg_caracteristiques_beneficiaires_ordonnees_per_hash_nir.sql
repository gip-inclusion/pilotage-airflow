select
    carac_benef.hash_nir,
    carac_benef.genre_salarie,
    carac_benef.contrat_salarie_ass,
    carac_benef.contrat_salarie_aah,
    carac_benef.contrat_salarie_rqth,
    carac_benef.salarie_adr_is_zrr,
    carac_benef.duree_inscription_ft,
    carac_benef.beneficiaires_du_rsa,
    carac_benef.salarie_qpv,
    carac_benef.tranche_age_beneficiaires,
    carac_benef.niveau_de_formation,
    row_number() over (
        partition by carac_benef.hash_nir
        order by carac_benef.contrat_date_embauche desc
    ) as rang
from {{ ref("caracteristiques_beneficiaires") }} as carac_benef
