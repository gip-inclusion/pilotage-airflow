select
    emi.emi_afi_id,
    emi.emi_pph_id,
    emi.emi_sme_mois,
    emi.emi_sme_annee,
    emi.emi_sme_version,
    emi.emi_dsm_id,
    case
        when ctr.contrat_salarie_rsa = 'OUI-M' then 'RSA majoré'
        when ctr.contrat_salarie_rsa = 'OUI-NM' then 'RSA non majoré'
        else 'Non bénéficiaire du RSA'
    end
    as majoration_brsa,
    case
        when ctr.contrat_salarie_rsa = 'OUI-M' then 'OUI'
        when ctr.contrat_salarie_rsa = 'OUI-NM' then 'OUI'
        else 'NON'
    end
    as salarie_brsa,
    sum(emi.emi_nb_heures_travail) as nombre_heures_travaillees,
    count(ctr.contrat_salarie_rsa) as nombre_salaries
from
    {{ ref('eph_dates_etat_mensuel_individualise') }} as constantes
cross join {{ ref('fluxIAE_EtatMensuelIndiv_v2') }} as emi
left join {{ ref('fluxIAE_ContratMission_v2') }} as ctr
    on ctr.contrat_id_ctr = emi.emi_ctr_id
where
    emi.emi_sme_annee >= constantes.annee_en_cours_2
group by
    emi.emi_afi_id,
    emi.emi_pph_id,
    emi.emi_sme_mois,
    emi.emi_sme_annee,
    emi.emi_sme_version,
    emi.emi_dsm_id,
    ctr.contrat_salarie_rsa
